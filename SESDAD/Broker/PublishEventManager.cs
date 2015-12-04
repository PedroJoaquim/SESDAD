using System;
using System.Collections.Generic;
using System.Linq;
using Shared_Library;
using System.Threading.Tasks;

namespace Broker
{
    abstract class PublishEventManager
    {

        private Dictionary<string, List<int>> receivedEvents = new Dictionary<string, List<int>>();
        private Broker b;

        internal Broker B
        {
            get
            {
                return b;
            }

            set
            {
                b = value;
            }
        }

        public Dictionary<string, List<int>> ReceivedEvents
        {
            get
            {
                return receivedEvents;
            }

            set
            {
                receivedEvents = value;
            }
        }

        protected PublishEventManager(Broker b)
        {
            this.B = b;
        }

        public abstract void ExecuteDistribution(string sourceSite, string sourceEntity, Event e, int seqNumber);
        protected abstract int GetOutgoingSeqNumber(string brokerName, string pName);
        public abstract void PublishStoredEvents(List<StoredEvent> storedEvents);

        //function that gets the interessed entities depending on the routing policy
        protected List<string> GetInteressedEntities(Event e, bool filter)
        {
            if (filter)
            {
                return b.ForwardingTable.GetAllInterestedEntities(e.Topic);
            }
            else
            {
                return Utils.MergeListsNoRepetitions(b.RemoteNetwork.GetAllOutSites(), b.ForwardingTable.GetAllInterestedEntities(e.Topic));
            }

        }

        public abstract void EventDispatchedByMainServer(StoredEvent old);

        //function to send the publish event to other brokers or subscribers
        protected void ProcessEventRouting(List<string> interessedEntities, Event e, string sourceSite)
        {
            bool logDone = false;

            /*
             * Distribute messages to all interessed subscribers
             */


            foreach (KeyValuePair<string, IRemoteSubscriber> entry in b.RemoteNetwork.Subscribers)
            {
                if (interessedEntities.Contains(entry.Key))
                {
                    if (!logDone && b.SysConfig.LogLevel.Equals(SysConfig.FULL))
                    {
                        b.PuppetMaster.LogEventForwarding(b.Name, e.Publisher, e.Topic, e.EventNr);
                        logDone = true;
                    }

                    Console.WriteLine(String.Format("[EVENT DELIVERED] Topic: {0} Publisher: {1} EventNr: {2} To: {3}", e.Topic, e.Publisher, e.EventNr, entry.Key));
                    try
                    { entry.Value.NotifyEvent(e); }
                    catch(Exception) { /*ignore*/ }

                }
            }
            /*
             * Now forward messages to interessed brokers (that can fail)
             */
            
            List<Tuple<string, int>> interessedSitesInfo = new List<Tuple<string, int>>();

            foreach (string site in b.RemoteNetwork.GetAllOutSites())
            {
                if (!site.Equals(sourceSite) && interessedEntities.Contains(site))
                {
                    if (!logDone && b.SysConfig.LogLevel.Equals(SysConfig.FULL))
                    {
                        b.PuppetMaster.LogEventForwarding(b.Name, e.Publisher, e.Topic, e.EventNr);
                        logDone = true;
                    }

                    interessedSitesInfo.Add(new Tuple<string, int>(site, GetOutgoingSeqNumber(site, e.Publisher)));
                }
            }

            if (interessedSitesInfo.Count > 0)
            {
                int actionID = b.FManager.FMMultiplePublishEvent(e, interessedSitesInfo); //send to all
                b.FManager.WaitEventDistribution(actionID); //wait that events are forwarded
            }
        }

 

        protected bool AlreadyProcessedEvent(Event e)
        {
            List<int> targetEvents;
            string source = e.Publisher;
            int seqNumber = e.EventNr;

            lock(ReceivedEvents)
            {
                if(!ReceivedEvents.ContainsKey(source))
                {
                    ReceivedEvents[source] = new List<int>();
                    ReceivedEvents[source].Add(seqNumber);
                    return false;
                }
                else
                {
                    targetEvents = ReceivedEvents[source];
                }
            }

            lock(targetEvents)
            {
                if (targetEvents.Contains(seqNumber))
                    return true;
                else
                {
                    targetEvents.Add(seqNumber);
                    return false;
                }
            }
        }

        
    }

    class NoOrderPublishEventManager : PublishEventManager
    {
        
        public NoOrderPublishEventManager(Broker b) : base(b) { }

        public override void ExecuteDistribution(string sourceSite, string sourceEntity, Event e, int seqNumber)
        {
            if (AlreadyProcessedEvent(e))
                return;

            List<string> interessedEntities = GetInteressedEntities(e, B.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER));
            bool sendACK = e.SendACK;

            e.SendACK = true;
            ProcessEventRouting(interessedEntities, e, sourceSite);

            if (sendACK)
                B.FManager.SendEventDispatchedAsync(e.EventNr, e.Publisher);
        }

        public override void PublishStoredEvents(List<StoredEvent> storedEvents)
        {
            foreach (StoredEvent item in storedEvents)
            {
                StoredEvent itemCopy = item;
                itemCopy.E.SendACK = false;
                new Task(() => ExecuteDistribution(itemCopy.SourceSite, item.SourceEntity, itemCopy.E, itemCopy.InSeqNumber)).Start();
            }
        }


        protected override int GetOutgoingSeqNumber(string siteName, string pName)
        {
            return 1; //irrelevant for no order
        }

        public override void EventDispatchedByMainServer(StoredEvent old)
        {
            //ignore no actions need to be performed
        }
    }

    class FIFOPublishEventManager : PublishEventManager
    {
        //1 string = source remote site name       2 string = publisher name
        private Dictionary<string, Dictionary<string, PublishEventsStorage>> inTable = new Dictionary<string, Dictionary<string, PublishEventsStorage>>();
        private Dictionary<string, Dictionary<string, int>> outTable = new Dictionary<string, Dictionary<string, int>>();

        public FIFOPublishEventManager(Broker b) : base(b) { }

        public override void ExecuteDistribution(string sourceSite, string sourceEntity, Event e, int seqNumber)
        {
            List<string> interessedEntities;
            Event outgoingEvent;
            PublishEventsStorage storedEvents = GetCreateEventOrder(sourceEntity, e.Publisher);
            bool sendACK;


            if (AlreadyProcessedEvent(e))
                return;

            lock (storedEvents)
            {
                storedEvents.InsertInOrder(e, seqNumber);

                while (storedEvents.CanSendEvent())
                {
                    
                    outgoingEvent = storedEvents.GetFirstEvent();
                    sendACK = outgoingEvent.SendACK;
                    outgoingEvent.SendACK = true;

                    interessedEntities = GetInteressedEntities(outgoingEvent, B.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER));
                    ProcessEventRouting(interessedEntities, outgoingEvent, sourceSite);

                    if(sendACK)
                        B.FManager.SendEventDispatchedAsync(outgoingEvent.EventNr, outgoingEvent.Publisher);

                    storedEvents.FirstEventSend();
                }


            }
        }

        public override void PublishStoredEvents(List<StoredEvent> storedEvents)
        {

            IDictionary<string, IDictionary<string, int>> minNumbers = new Dictionary<string, IDictionary<string, int>>();

            foreach (StoredEvent item in storedEvents)
            {
                if(!minNumbers.ContainsKey(item.SourceEntity))
                {
                    minNumbers[item.SourceEntity] = new Dictionary<string, int>();
                    minNumbers[item.SourceEntity][item.E.Publisher] = item.InSeqNumber;
                }
                else if (!minNumbers[item.SourceEntity].ContainsKey(item.E.Publisher))
                {
                    minNumbers[item.SourceEntity][item.E.Publisher] = item.InSeqNumber;
                }
                else if(item.InSeqNumber < minNumbers[item.SourceEntity][item.E.Publisher])
                {
                    minNumbers[item.SourceEntity][item.E.Publisher] = item.InSeqNumber;
                }
            }

            //set the nextSeqNumber as the min off the storedEvents
            foreach (KeyValuePair<string, IDictionary<string, int>> item in minNumbers)
            {
                foreach (KeyValuePair<string, int> item2 in item.Value)
                {
                    PublishEventsStorage se = GetCreateEventOrder(item.Key, item2.Key);
                    se.NextSeqNumber = item2.Value;
                }
            }

            foreach (StoredEvent item in storedEvents)
            {
                StoredEvent itemCopy = item;
                itemCopy.E.SendACK = false;
                new Task(() => ExecuteDistribution(itemCopy.SourceSite, item.SourceEntity, itemCopy.E, itemCopy.InSeqNumber)).Start();
            }
        }

        public override void EventDispatchedByMainServer(StoredEvent old)
        {
            PublishEventsStorage storedEvents = GetCreateEventOrder(old.SourceEntity, old.E.Publisher);

            lock (storedEvents)
            {
                if (storedEvents.NextSeqNumber <= old.InSeqNumber)
                {
                    storedEvents.NextSeqNumber = old.InSeqNumber + 1;
                }
            }
        }

        private PublishEventsStorage GetCreateEventOrder(string sourceName, string publisherName)
        {
            PublishEventsStorage result;
            string cleanRemoteName = sourceName.ToLower();
            string cleanPName = publisherName.ToLower();

            lock (this.inTable)
            {
                if (!this.inTable.ContainsKey(cleanRemoteName))
                    this.inTable[cleanRemoteName] = new Dictionary<string, PublishEventsStorage>();

                if (!this.inTable[cleanRemoteName].ContainsKey(cleanPName))
                    this.inTable[cleanRemoteName][cleanPName] = new PublishEventsStorage();

                result = this.inTable[cleanRemoteName][cleanPName];
            }

            return result;
        }

        protected override int GetOutgoingSeqNumber(string siteName, string pName)
        {
            string cleanSiteName = siteName.ToLower();
            string cleanPName = pName.ToLower();
            int result;

            lock (this.outTable)
            {
                if (!this.outTable.ContainsKey(cleanSiteName))
                    this.outTable[cleanSiteName] = new Dictionary<string, int>();

                if (!this.outTable[cleanSiteName].ContainsKey(cleanPName))
                    this.outTable[cleanSiteName][cleanPName] = 1;

                result = this.outTable[cleanSiteName][cleanPName];
                this.outTable[cleanSiteName][cleanPName] = result + 1;
            }

            return result;
        }


    }


    class TotalOrderPublishEventManager : PublishEventManager
    {

        private Dictionary<string, Dictionary<string, PublishEventsStorage>> inTable = new Dictionary<string, Dictionary<string, PublishEventsStorage>>();
        private Dictionary<string, Dictionary<string, int>> outTable = new Dictionary<string, Dictionary<string, int>>();


        public TotalOrderPublishEventManager (Broker b): base(b) { }

        public override void ExecuteDistribution(string sourceSite, string sourceEntity, Event e, int seqNumber)
        {

            if (e.IsSequencerMessage)
            {
                ExecuteSequencerMessageDistribution(sourceSite, sourceEntity, e, seqNumber);
                return;
            }
                
            if (AlreadyProcessedEvent(e))
                return;

            List<string> interessedEntities = GetInteressedEntities(e, B.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER));

            bool sendACK = e.SendACK;

            e.SendACK = true;
            ProcessEventRouting(interessedEntities, e, sourceSite);

            if (sendACK)
                B.FManager.SendEventDispatchedAsync(e.EventNr, e.Publisher);
        }

        private void ExecuteSequencerMessageDistribution(string sourceSite, string sourceEntity, Event e, int seqNumber) //we have to ensure fifo order for sequencer messages
        {
            List<string> interessedEntities;
            Event outgoingEvent;
            PublishEventsStorage storedEvents = GetCreateEventOrder(sourceSite, sourceEntity);

 
            lock (storedEvents)
            {

                if (seqNumber == 1 && sourceEntity.Equals(Sequencer.SEQUENCER_PASSIVE_NAME)) //sequencer has changed reset outgoing seqNumbers
                    ResetOutSeqNumbers();

                storedEvents.InsertInOrder(e, seqNumber);

                while (storedEvents.CanSendEvent())
                {
                    outgoingEvent = storedEvents.GetFirstEvent();
                    interessedEntities = GetInteressedEntities(outgoingEvent, B.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER));
                    DifundSequencerMessage(interessedEntities, outgoingEvent, sourceSite, sourceEntity);
                    storedEvents.FirstEventSend();

                    if(B.IsSequencer)
                    {
                        if (!B.FManager.PassiveDead && B.IsSequencer) // send ack for sequencer replication server
                        {
                            try { B.FManager.PassiveServer.SequencerEventDispatched(outgoingEvent.EventNr, outgoingEvent.Publisher); } 
                            catch (Exception) { B.FManager.PassiveDead = true; }
                        }
                    }
                }
            }


        }

        private void ResetOutSeqNumbers()
        {
            lock(outTable)
            {
                List<Tuple<string, string>> valuesToChange = new List<Tuple<string, string>>();

                foreach (KeyValuePair<string, Dictionary<string, int>> outTableEntry in outTable)
                {
                    foreach (KeyValuePair<string, int> entry in outTableEntry.Value)
                    {
                        valuesToChange.Add(new Tuple<string, string>(outTableEntry.Key, entry.Key));
                    }
                }

                foreach (Tuple<string, string> tp in valuesToChange)
                {
                    outTable[tp.Item1][tp.Item2] = 1;
                }
            }

        }

        private void DifundSequencerMessage(List<string> interessedEntities, Event e, string sourceSite, string sourceEntity)
        {

            /*
             * Distribute messages to all interessed subscribers
             */

            foreach (KeyValuePair<string, IRemoteSubscriber> entry in B.RemoteNetwork.Subscribers)
            {
                if (interessedEntities.Contains(entry.Key))
                {
                    try { entry.Value.SequenceMessage(e.Publisher, e.EventNr); } catch (Exception) { }
                }
            }

            /*
             * Distribute messages to all interessed brokers
             */

            foreach (string site in B.RemoteNetwork.GetAllOutSites())
            {
                if (!site.Equals(sourceSite) && interessedEntities.Contains(site))
                {
                    int outSeqNumber = GetOutgoingSeqNumber(site, Sequencer.SEQUENCER_BASE_NAME);

                    foreach (IRemoteBroker broker in B.RemoteNetwork.OutBrokers[site])
                    {
                        string brokerName = B.RemoteNetwork.GetBrokerName(broker);

                        if (B.FManager.IsDead(site, brokerName))
                            continue;

                        try { broker.DifundSequencerMessage(e, B.RemoteNetwork.SiteName, sourceEntity, outSeqNumber); }
                        catch(Exception) { B.FManager.MarkAsDead(site, brokerName); }
                    }
                }
            }
        }

        protected override int GetOutgoingSeqNumber(string siteName, string pName)
        {
            string cleanSiteName = siteName.ToLower();
            string cleanPName = pName.ToLower();
            int result;

            lock (this.outTable)
            {
                if (!this.outTable.ContainsKey(cleanSiteName))
                    this.outTable[cleanSiteName] = new Dictionary<string, int>();

                if (!this.outTable[cleanSiteName].ContainsKey(cleanPName))
                    this.outTable[cleanSiteName][cleanPName] = 1;

                result = this.outTable[cleanSiteName][cleanPName];
                this.outTable[cleanSiteName][cleanPName] = result + 1;
            }

            return result;
        }

        public override void PublishStoredEvents(List<StoredEvent> storedEvents)
        {
            foreach (StoredEvent item in storedEvents)
            {
                StoredEvent itemCopy = item;
                itemCopy.E.SendACK = false;
                new Task(() => ExecuteDistribution(itemCopy.SourceSite, item.SourceEntity, itemCopy.E, itemCopy.InSeqNumber)).Start();
            }
        }

        public override void EventDispatchedByMainServer(StoredEvent old)
        {
            //ignore
        }

        private PublishEventsStorage GetCreateEventOrder(string sourceName, string publisherName)
        {
            PublishEventsStorage result;
            string cleanRemoteName = sourceName.ToLower();
            string cleanPName = publisherName.ToLower();

            lock (this.inTable)
            {
                if (!this.inTable.ContainsKey(cleanRemoteName))
                    this.inTable[cleanRemoteName] = new Dictionary<string, PublishEventsStorage>();

                if (!this.inTable[cleanRemoteName].ContainsKey(cleanPName))
                    this.inTable[cleanRemoteName][cleanPName] = new PublishEventsStorage();

                result = this.inTable[cleanRemoteName][cleanPName];
            }

            return result;
        }

    }

    class PublishEventsStorage
    {
        private List<Tuple<Event, int>> storedEvents = new List<Tuple<Event, int>>();
        private int nextSeqNumber;

        public int NextSeqNumber
        {
            get
            {
                return nextSeqNumber;
            }

            set
            {
                nextSeqNumber = value;
            }
        }

        public PublishEventsStorage()
        {
            this.NextSeqNumber = 1;
        }

        public void InsertInOrder(Event e, int inSeqNumber)
        {
            if (inSeqNumber < NextSeqNumber)
                return; //discard old publish events

            this.storedEvents.Add(new Tuple<Event, int>(e, inSeqNumber));
            storedEvents.Sort((x, y) => x.Item2.CompareTo(y.Item2));

        }

        public bool CanSendEvent()
        {
            return this.storedEvents.Count > 0 && this.storedEvents.ElementAt(0).Item2 == this.NextSeqNumber;
        }

        public Event GetFirstEvent()
        {
            return this.storedEvents.Count == 0 ? null : this.storedEvents.ElementAt(0).Item1;
        }

        public void FirstEventSend()
        {
            this.storedEvents.RemoveAt(0);
            this.NextSeqNumber++;
        }

    }
}
