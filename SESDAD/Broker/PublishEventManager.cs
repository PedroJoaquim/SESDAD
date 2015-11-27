using System;
using System.Collections.Generic;
using System.Linq;
using Shared_Library;

namespace Broker
{
    abstract class PublishEventManager
    {

        public abstract void ExecuteDistribution(Broker b, string sourceSite, Event e, int seqNumber);
        protected abstract int GetOutgoingSeqNumber(string brokerName, string pName);

        //function that gets the interessed entities depending on the routing policy
        protected List<string> GetInteressedEntities(Broker b, Event e, bool filter)
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

        //function to send the publish event to other brokers or subscribers
        protected void ProcessEventRouting(Broker broker, List<string> interessedEntities, Event e, string sourceSite)
        {
            bool logDone = false;

            /*
             * Distribute messages to all interessed subscribers
             */
            foreach (KeyValuePair<string, IRemoteSubscriber> entry in broker.RemoteNetwork.Subscribers)
            {
                if (interessedEntities.Contains(entry.Key))
                {
                    if (!logDone && broker.SysConfig.LogLevel.Equals(SysConfig.FULL))
                    {
                        broker.PuppetMaster.LogEventForwarding(broker.Name, e.Publisher, e.Topic, e.EventNr);
                        logDone = true;
                    }

                    entry.Value.NotifyEvent(e);
                }
            }
            /*
             * Now forward messages to interessed brokers (that can fail)
             */

            List<Tuple<string, int>> interessedSitesInfo = new List<Tuple<string, int>>();

            foreach (string site in broker.RemoteNetwork.GetAllOutSites())
            {
                if (!site.Equals(sourceSite) && interessedEntities.Contains(site))
                {
                    if (!logDone && broker.SysConfig.LogLevel.Equals(SysConfig.FULL))
                    {
                        broker.PuppetMaster.LogEventForwarding(broker.Name, e.Publisher, e.Topic, e.EventNr);
                        logDone = true;
                    }

                    interessedSitesInfo.Add(new Tuple<string, int>(site, GetOutgoingSeqNumber(site, e.Publisher)));
                }
            }

            if (interessedSitesInfo.Count > 0)
            {
                int actionID = broker.FManager.FMMultiplePublishEvent(e, interessedSitesInfo); //send to all
                broker.FManager.WaitEventDistribution(actionID); //wait that events are forwarded
            }
        }
    }

    class NoOrderPublishEventManager : PublishEventManager
    {
        
        public override void ExecuteDistribution(Broker b, string sourceSite, Event e, int seqNumber)
        {
            List<string> interessedEntities = GetInteressedEntities(b, e, b.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER));
            ProcessEventRouting(b, interessedEntities, e, sourceSite);
            b.FManager.EventDispatched(e.EventNr, e.Publisher);
        }

        protected override int GetOutgoingSeqNumber(string siteName, string pName)
        {
            return 1; //irrelevant for no order
        }
    }

    class FIFOPublishEventManager : PublishEventManager
    {
        //1 string = source remote site name       2 string = publisher name
        private Dictionary<string, Dictionary<string, PublishEventsStorage>> inTable = new Dictionary<string, Dictionary<string, PublishEventsStorage>>();
        private Dictionary<string, Dictionary<string, int>> outTable = new Dictionary<string, Dictionary<string, int>>();


        public override void ExecuteDistribution(Broker b, string sourceSite, Event e, int seqNumber)
        {
            List<string> interessedEntities;
            Event outgoingEvent;
            PublishEventsStorage storedEvents = GetCreateEventOrder(sourceSite, e.Publisher);

            lock (storedEvents)
            {
                storedEvents.InsertInOrder(e, seqNumber);
                
                while(storedEvents.CanSendEvent())
                {
                    outgoingEvent = storedEvents.GetFirstEvent();
                    interessedEntities = GetInteressedEntities(b, outgoingEvent, b.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER));
                    ProcessEventRouting(b, interessedEntities, outgoingEvent, sourceSite);
                    b.FManager.EventDispatched(outgoingEvent.EventNr, outgoingEvent.Publisher);
                    storedEvents.FirstEventSend();
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
 
        public override void ExecuteDistribution(Broker b, string source, Event e, int seqNumber)
        {
            throw new NotImplementedException();
        }

        protected override int GetOutgoingSeqNumber(string brokerName, string pName)
        {
            throw new NotImplementedException();
        }
    }

    class PublishEventsStorage
    {
        private List<Tuple<Event, int>> storedEvents = new List<Tuple<Event, int>>();
        private int nextSeqNumber;

        public PublishEventsStorage()
        {
            this.nextSeqNumber = 1;
        }

        public void InsertInOrder(Event e, int inSeqNumber)
        {
            if (inSeqNumber < nextSeqNumber) return; //discard old publish events

            this.storedEvents.Add(new Tuple<Event, int>(e, inSeqNumber));
            storedEvents.Sort((x, y) => x.Item2.CompareTo(y.Item2));
        }

        public bool CanSendEvent()
        {
            return this.storedEvents.Count > 0 && this.storedEvents.ElementAt(0).Item2 == this.nextSeqNumber;
        }

        public Event GetFirstEvent()
        {
            return this.storedEvents.Count == 0 ? null : this.storedEvents.ElementAt(0).Item1;
        }

        public void FirstEventSend()
        {
            this.storedEvents.RemoveAt(0);
            this.nextSeqNumber++;
        }
    }
}
