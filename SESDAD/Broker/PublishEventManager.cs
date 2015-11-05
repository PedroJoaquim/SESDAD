using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Shared_Library;
namespace Broker
{
    abstract class PublishEventManager
    {
        private string myName;
        
        public string MyName
        {
            get
            {
                return myName;
            }

            set
            {
                myName = value;
            }
        }

        public PublishEventManager(string sourceName)
        {
            this.MyName = sourceName;
        }

        public abstract void ExecuteDistribution(Broker b, string source, Event e, int seqNumber);
        protected abstract int GetOutgoingSeqNumber(string brokerName, string pName);

        //function that gets the interessed entities depending on the routing policy
        protected List<string> GetInteressedEntities(Broker b, Event e, bool filter)
        {
            List<string> result = new List<string>();
            List<string> interessed = b.ForwardingTable.GetAllInterestedEntities(e.Topic);

            if (filter)
            {
                return interessed;
            }

            foreach (string item in interessed)
            {
                result.Add(item);
            }

            foreach (string item in b.Brokers.Keys.ToList())
            {
                if (!result.Contains(item))
                    result.Add(item);
            }

            return result;
        }
       
        //function to send the publish event to other brokers or subscribers
        protected void ProcessEventRouting(Broker broker, List<string> interessedEntities, Event e, string source)
        {
            bool entityFound = false;
            bool logDone = false;
            int outNumber;


            foreach (string entityName in interessedEntities)
            {
                entityFound = false;

                foreach (KeyValuePair<string, IRemoteBroker> entry in broker.Brokers)
                {
                    if (entry.Key.Equals(entityName))
                    {
                        if (!entry.Key.Equals(source))
                        {

                            if (!logDone && broker.SysConfig.LogLevel.Equals(SysConfig.FULL))
                            {
                                broker.PuppetMaster.LogEventForwarding(broker.Name, e.Publisher, e.Topic, e.EventNr);
                                logDone = true;
                            }

                            outNumber = GetOutgoingSeqNumber(entry.Key, e.Publisher);
                            new Task(() =>
                            {
                                entry.Value.DifundPublishEvent(e, this.MyName, outNumber);
                            }).Start();
                            
                        }

                        entityFound = true;
                        break;
                    }

                }

                if (entityFound) continue;

                foreach (KeyValuePair<string, IRemoteSubscriber> entry in broker.Subscribers)
                {
                    if (entry.Key.Equals(entityName))
                    {
                        if (!logDone && broker.SysConfig.LogLevel.Equals(SysConfig.FULL))
                        {
                            broker.PuppetMaster.LogEventForwarding(broker.Name, e.Publisher, e.Topic, e.EventNr);
                            logDone = true;
                        }
                        entry.Value.NotifyEvent(e);
                    }
                }
            }

        }

    }

    class NoOrderPublishEventManager : PublishEventManager
    {
        public NoOrderPublishEventManager(string myName) : base(myName) { }

        public override void ExecuteDistribution(Broker b, string source, Event e, int seqNumber)
        {
            List<string> interessedEntities = GetInteressedEntities(b, e, b.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER));
            ProcessEventRouting(b, interessedEntities, e, source);
        }

        protected override int GetOutgoingSeqNumber(string brokerName, string pName)
        {
            return 1; //irrelevant for no order
        }
    }

    class FIFOPublishEventManager : PublishEventManager
    {
        //1 string = source remote entity name       2 - string publisher name
        private Dictionary<string, Dictionary<string, PublishEventsStorage>> inTable = new Dictionary<string, Dictionary<string, PublishEventsStorage>>();
        private Dictionary<string, Dictionary<string, int>> outTable = new Dictionary<string, Dictionary<string, int>>();

        public FIFOPublishEventManager(string myName) : base(myName) { }


        public override void ExecuteDistribution(Broker b, string source, Event e, int seqNumber)
        {
            List<string> interessedEntities;
            Event outgoingEvent;
            PublishEventsStorage storedEvents = GetCreateEventOrder(source, e.Publisher);

            lock(storedEvents)
            {
                storedEvents.InsertInOrder(e, seqNumber);
                
                while(storedEvents.CanSendEvent())
                {
                    outgoingEvent = storedEvents.GetFirstEvent();
                    interessedEntities = GetInteressedEntities(b, outgoingEvent, b.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER));
                    ProcessEventRouting(b, interessedEntities, outgoingEvent, source);
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

        protected override int GetOutgoingSeqNumber(string brokerName, string pName)
        {
            string cleanBrokerName = brokerName.ToLower();
            string cleanPName = pName.ToLower();
            int result;

            lock (this.outTable)
            {
                if (!this.outTable.ContainsKey(cleanBrokerName))
                    this.outTable[cleanBrokerName] = new Dictionary<string, int>();

                if (!this.outTable[cleanBrokerName].ContainsKey(cleanPName))
                    this.outTable[cleanBrokerName][cleanPName] = 1;

                result = this.outTable[cleanBrokerName][cleanPName];
                this.outTable[cleanBrokerName][cleanPName] = result + 1;
            }

            return result;
        }

    }


    class TotalOrderPublishEventManager : PublishEventManager
    {
        public TotalOrderPublishEventManager(string myName) : base(myName) {  }

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
