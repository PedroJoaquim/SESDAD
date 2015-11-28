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

        private int actionID;
        private bool passiveDead;
        private IDictionary<int, EventInfo> waitingEvents;
        private IDictionary<int, int> timeoutIDMap; //maps timeoutids for actionsID
        private ReplicationStorage repStorage;

        private Object actionIDObject; //lock for actionID 


        public ReplicationStorage RepStorage
        {
            get
            {
                return repStorage;
            }

            set
            {
                repStorage = value;
            }
        }

        public BrokerFaultManager(RemoteEntity re) : base(re, QUEUE_SIZE, NUM_THREADS)
        {
            actionID = 1;
            this.actionIDObject = new Object();
            this.waitingEvents = new Dictionary<int, EventInfo>();
            this.timeoutIDMap = new Dictionary<int, int>();
            this.RepStorage = new ReplicationStorage();
            this.passiveDead = false;
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

        public void NewEventArrived(Event e, int timeoutID, string sourceEntity, string sourceSite)
        {
            RemoteEntity.CheckFreeze();

            IRemoteBroker passiveServer = GetPassiveServer(e.Publisher);

            if (!passiveDead)
            {
                try
                {
                    passiveServer.StoreNewEvent(e);
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
         * Passive redundancy -- store the new event
         */

        internal void StoreNewEvent(Event e)
        {
            RemoteEntity.CheckFreeze();
            this.RepStorage.StoreNewEvent(e);
        }

        /*
         * Passive redundancy -- event dispacted
         */

        internal void EventDispatched(int eventNr, string publisher) //when we are passive server
        {
            RemoteEntity.CheckFreeze();
            this.RepStorage.EventDispatched(eventNr, publisher);
        }

        /*
         * Passive redundancy -- send to the passive server
         */

        public void SendEventDispatchedAsync(int eventNr, string publisher)
        {
            this.Events.Produce(new EventDispatchedCommand(eventNr, publisher, GetPassiveServer(publisher)));
        }

        private IRemoteBroker GetPassiveServer(string publisher)
        {
            int passiveServer = Utils.CalcBrokerForwardIndex(3, publisher, false) % 2; //mathematical property that assures that we pick the same
            return RemoteEntity.RemoteNetwork.InBrokersList[passiveServer];
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

        public override void ActionTimedout(DifundPublishEventProperties p)
        {
            int actionID;

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

    public class ReplicationStorage
    {

        IDictionary<string, StoredEvents> storedEvents;

        public ReplicationStorage()
        {
            this.storedEvents = new Dictionary<string, StoredEvents>();
        }

        public void StoreNewEvent(Event e)
        {

            StoredEvents se;
            string publisher = e.Publisher;

            lock (this)
            {
                if (!storedEvents.ContainsKey(publisher))
                    storedEvents[publisher] = new StoredEvents();

                se = storedEvents[publisher];
            }

            se.StoreNewEvent(e);
        }

        public void EventDispatched(int eventNr, string publisher)
        {
            StoredEvents se;

            lock (this)
            {
                se = storedEvents[publisher];
            }

            se.EventDispatched(eventNr);
        }


        public bool HasPreviousEventsToSend(int eventNr, string publisher)
        {
            StoredEvents se;

            lock (this)
            {
                se = storedEvents[publisher];
            }

            return se.HasPreviousEventsToSend(eventNr);
        }

        public List<Event> GetPendindEvents(bool all, string publisher)
        {
            StoredEvents se;

            lock (this)
            {
                se = storedEvents[publisher];
            }

            return se.GetPendindEvents(all);
        }

    }

    public class StoredEvents
    {
        private IDictionary<int , bool> storedEventsDelivered;   
        private IDictionary<int, Event> storedEvents;
        private List<int> storedEventsIndex;

        public StoredEvents()
        {
            storedEventsDelivered = new Dictionary<int, bool>();
            storedEvents = new Dictionary<int, Event>();
            storedEventsIndex = new List<int>();
            
        }

        public void StoreNewEvent(Event e)
        {
            lock(this)
            {
                storedEvents[e.EventNr] = e;
                storedEventsDelivered[e.EventNr] = false;
                storedEventsIndex.Add(e.EventNr);
                storedEventsIndex.Sort((x, y) => x.CompareTo(y));
            }
        }

        public void EventDispatched(int eventNr)
        {
            lock (this)
            {
                storedEvents.Remove(eventNr); //discard event
                storedEventsDelivered[eventNr] = true;
            }
        }


        public bool HasPreviousEventsToSend(int eventNr)
        {
            lock(this)
            {
                int i = storedEventsIndex.Count - 1;
                return storedEventsIndex.Count > 0 && storedEventsIndex[i] == eventNr - 1 && !storedEventsDelivered[storedEventsIndex[i]];
            }
        }

        public List<Event> GetPendindEvents(bool all)
        {
            List<Event> result = new List<Event>();

            for(int i = storedEventsIndex.Count - 1; i >= 0; i--)
            {
                if (!storedEventsDelivered[storedEventsIndex[i]])
                {
                    result.Add(storedEvents[storedEventsIndex[i]]);
                    storedEvents.Remove(storedEventsIndex[i]);
                    storedEventsDelivered[storedEventsIndex[i]] = true;
                }
                else
                {
                    if (!all)
                        break;
                } 

            }

            return result;
        }
    }
}
