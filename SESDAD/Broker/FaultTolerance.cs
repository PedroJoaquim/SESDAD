using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;
using System.Threading;

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

        public void RemoveTimeoutID(int timeoutID)
        {
            lock (timeoutIDs)
            {
                timeoutIDs.Remove(timeoutID);
            }
        }

        public void WaitAll()
        {
            lock(timeoutIDs)
            {
                while (totalACKs < receivedACKs)
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
        private int actionID;
        private Dictionary<int, EventInfo> waitingEvents;
        private Dictionary<int, int> timeoutIDMap; //maps timeoutids for actionsID

        private Object actionIDObject; //lock for actionID 
    
        public BrokerFaultManager(RemoteEntity re) : base(re)
        {
            actionID = 1;
            this.actionIDObject = new Object();
            this.waitingEvents = new Dictionary<int, EventInfo>();
            this.timeoutIDMap = new Dictionary<int, int>();
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

        /*
         * First call to every broker, we have to send all before we begin accpeting acks
         */

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


                int timeoutID = TMonitor.NewActionPerformed(e, outSeqNumber, site);
                eventInfo.AddNewTimeout(timeoutID);

                lock(timeoutIDMap)
                {
                    timeoutIDMap[timeoutID] = actionID;
                }

                
                new Task(() => { ExecuteEventTransmission(e, site, outSeqNumber, timeoutID, false); }).Start();
            }


            return actionID;
        }

        /*
         * Restransmission event
         */
        private void FMPublishEventRetransmission(Event e, string targetSite, int outSeqNumber, int actionID, int oldTimeoutID)
        {
            EventInfo eventInfo;
            int newTimeoutID;

            eventInfo = GetEventInfoTS(actionID);

            if (eventInfo.AlreadyReceivedACK(oldTimeoutID))
                return; //acked already received, possible desync

            eventInfo.RemoveTimeoutID(oldTimeoutID);
            newTimeoutID = TMonitor.NewActionPerformed(e, outSeqNumber, targetSite);
            eventInfo.AddNewTimeout(newTimeoutID);

            lock (timeoutIDMap)
            {
                timeoutIDMap.Remove(oldTimeoutID);
                timeoutIDMap[newTimeoutID] = actionID;
            }

            new Task(() => { ExecuteEventTransmission(e, targetSite, outSeqNumber, newTimeoutID, true); }).Start();
            
        }

        private void ExecuteEventTransmission(Event e, string targetSite, int outSeqNumber, int timeoutID, bool retransmission)
        {
            RemoteEntity.RemoteNetwork.ChooseBroker(targetSite, e.Publisher, retransmission).DifundPublishEvent(e, RemoteEntity.RemoteNetwork.SiteName, RemoteEntity.Name, outSeqNumber, timeoutID);
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
            lock(this)
            {
                waitingEvents.Remove(actionID);
            }

            lock(timeoutIDMap)
            {
                List<int> toBeRemoved = new List<int>();

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
            lock(timeoutIDMap)
            {
                return timeoutIDMap[timeoutID];
            }
        }
        /*
         * ITimeoutListener Interface Implementation
         */

        public override void ActionTimedout(DifundPublishEventProperties p)
        {
            int actionID;
            
            actionID = GetActionIDTS(p.Id);

            FMPublishEventRetransmission(p.E, p.TargetSite, p.OutSeqNumber, actionID, p.Id);
        }


        public override void ActionACKReceived(int timeoutID)
        {
            int actionID;
            EventInfo eventInfo;

            actionID = GetActionIDTS(timeoutID);
            eventInfo = GetEventInfoTS(actionID);
            eventInfo.PostACK(timeoutID);
        }

        public void SendACK(int timeoutID, string sourceEntity, string sourceSite)
        {
            new Task(() => { SendACKNewThread(sourceSite, sourceEntity, timeoutID); }).Start(); //send ack
        }

        private void SendACKNewThread(string sourceSite, string sourceEntity, int timeoutID)
        {
            RemoteEntity.CheckFreeze();

            if (sourceSite.Equals(RemoteEntity.RemoteNetwork.SiteName))
                RemoteEntity.RemoteNetwork.Publishers[sourceEntity].ReceiveACK(timeoutID);
            else
                RemoteEntity.RemoteNetwork.OutBrokersNames[sourceEntity].ReceiveACK(timeoutID);
        }

    }
}
