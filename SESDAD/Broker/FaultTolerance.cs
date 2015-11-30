using System;
using System.Collections.Generic;
using Shared_Library;
using System.Threading;
using System.Threading.Tasks;

namespace Broker
{
    class EventInfo
    {
        private int actionID;
        private int totalACKs;
        private int receivedACKs;
        private Dictionary<int, bool> timeoutIDs;

        public EventInfo(int actionID, int totalACKs)
        {
            this.actionID = actionID;
            this.totalACKs = totalACKs;
            this.receivedACKs = 0;
            this.timeoutIDs = new Dictionary<int, bool>();
        }

        public void AddNewTimeout(int timeoutID)
        {
            lock(timeoutIDs)
            {
                timeoutIDs[timeoutID] = false;
            }
        }

        public bool AlreadyReceivedACK(int timeoutID)
        {
            lock (timeoutIDs)
            {
                return timeoutIDs.ContainsKey(timeoutID) && timeoutIDs[timeoutID];
            }
        }

        public void WaitAll()
        {
            lock(timeoutIDs)
            {
                while (receivedACKs < totalACKs)
                {
                    Monitor.Wait(timeoutIDs);
                }
            }
        }

        public void PostACK(int timeoutID)
        {
            lock (timeoutIDs)
            {
                if (timeoutIDs[timeoutID])
                    return; //duplicated ACK

                timeoutIDs[timeoutID] = true;
                receivedACKs++;

                if (receivedACKs >= totalACKs)
                    Monitor.PulseAll(timeoutIDs);
            }
        }
    }

    class BrokerFaultManager : FaultManager
    {
        private const int NUM_THREADS = 25;
        private const int QUEUE_SIZE = 200;
        private const int HEARTH_BEATS_TIME = 3500;

        private int actionID;
        private bool passiveDead;
        private IDictionary<int, EventInfo> waitingEvents;
        private IDictionary<int, int> timeoutIDMap; //maps timeoutids for actionsID
        private IRemoteBroker passiveServer;

        private Object actionIDObject; //lock for actionID 



        public BrokerFaultManager(RemoteEntity re) : base(re, QUEUE_SIZE, NUM_THREADS)
        {
            actionID = 1;
            this.actionIDObject = new Object();
            this.waitingEvents = new Dictionary<int, EventInfo>();
            this.timeoutIDMap = new Dictionary<int, int>();
            this.passiveDead = false;

            Thread t = new Thread(SendHearthBeats); //send hearth beats
            t.Start();
        }

        public IRemoteBroker PassiveServer
        {
            get
            {
                return passiveServer;
            }

            set
            {
                passiveServer = value;
            }
        }


        private void SendHearthBeats()
        {
            bool replicaAlive = true;

            while(replicaAlive)
            {
               Thread.Sleep(HEARTH_BEATS_TIME);
               try { PassiveServer.HearthBeat(); }
               catch(Exception) { replicaAlive = false; }
            }
        }
        /*
         *  Method that returns a new actionID - threadsafe
         */
        public int NextActionID()
        {
            lock(actionIDObject)
            {
                int newID = actionID;
                this.actionID++;
                return newID;
            }
        }

        public void NewEventArrived(Event e, int timeoutID, string sourceEntity, string sourceSite, int inSeqNumber)
        {
            RemoteEntity.CheckFreeze();

            if (!passiveDead)
            {
                try
                {
                    PassiveServer.StoreNewEvent(e, sourceSite, inSeqNumber);
                } catch (Exception) { passiveDead = true; }
            }

            this.Events.Produce(new SendACKCommand(timeoutID, sourceEntity, sourceSite));
        }


        public int FMMultiplePublishEvent(Event e, List<Tuple<string, int>> targetSites)
        {
            int actionID = NextActionID();
            EventInfo eventInfo = new EventInfo(actionID, targetSites.Count);

            lock(this)
            {
                waitingEvents[actionID] = eventInfo;
            }

            foreach (Tuple<string, int> entry in targetSites)
            {
                string site = entry.Item1;
                int outSeqNumber = entry.Item2;
                this.Events.Produce(new ForwardEventCommand(e, site, outSeqNumber, actionID)); //async
            }

            return actionID;
        }

        public void RegisterNewTimeoutID(int timeoutID, int actionID)
        {
            lock (this)
            {
                waitingEvents[actionID].AddNewTimeout(timeoutID);
                timeoutIDMap[timeoutID] = actionID;
            }
        }

        /*
         * Restransmission event
         */
        private void FMPublishEventRetransmission(Event e, string targetSite, int outSeqNumber, int actionID, int oldTimeoutID)
        {
            EventInfo eventInfo = GetEventInfoTS(actionID);

            if (!eventInfo.AlreadyReceivedACK(oldTimeoutID))
            {
                this.Events.Produce(new ForwardEventRetransmissionCommand(e, targetSite, outSeqNumber, oldTimeoutID));
            }
        }

        /*
         * Passive redundancy -- send to the passive server
         */

        public void SendEventDispatchedAsync(int eventNr, string publisher)
        {
            this.Events.Produce(new EventDispatchedCommand(eventNr, publisher, PassiveServer));
        }

        /*
         * Wait untill all publish events are acked
         */

        public void WaitEventDistribution(int actionID)
        {
            EventInfo eventInfo;

            eventInfo = GetEventInfoTS(actionID);

            eventInfo.WaitAll();
            RemoveElements(actionID); 
        }

        private void RemoveElements(int actionID)
        {

            List<int> toBeRemoved = new List<int>();

            lock (this)
            {
                waitingEvents.Remove(actionID);

                foreach (KeyValuePair<int, int> item in timeoutIDMap)
                {
                    if (item.Value == actionID)
                        toBeRemoved.Add(item.Key);
                }

                foreach (int key in toBeRemoved)
                {
                    timeoutIDMap.Remove(key);
                }
            }
        }

        private EventInfo GetEventInfoTS(int actionID)
        {
            lock(this)
            {
                return waitingEvents[actionID];
            }
        }

        private int GetActionIDTS(int timeoutID)
        {
            lock (this)
            {
                return timeoutIDMap[timeoutID];
            }
        }
        
        /*
         * ITimeoutListener Interface Implementation
         */

        public override void ActionTimedout(ActionProperties ap)
        {
            int actionID;
            DifundPublishEventProperties p = (DifundPublishEventProperties)ap;

            actionID = GetActionIDTS(p.Id);
            IncMissedACKs(p.TargetSite, p.TargetEntity);
            FMPublishEventRetransmission(p.E, p.TargetSite, p.OutSeqNumber, actionID, p.Id);
        }


        public override void ActionACKReceived(int timeoutID, string entityName, string entitySite)
        {
            int actionID;
            EventInfo eventInfo;

            TMonitor.PostACK(timeoutID);
            ResetMissedACKs(entitySite, entityName);
            actionID = GetActionIDTS(timeoutID);
            eventInfo = GetEventInfoTS(actionID);
            eventInfo.PostACK(timeoutID);
        }
    }

    
}
