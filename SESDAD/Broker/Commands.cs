using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;

namespace Broker
{
    class DifundPublishEventCommand : Command
    {
        #region "Properties"
        private Event e;
        private string source;

        public Event E
        {
            get
            {
                return e;
            }

            set
            {
                e = value;
            }
        }

        public string Source
        {
            get
            {
                return source;
            }

            set
            {
                source = value;
            }
        }

        #endregion

        public DifundPublishEventCommand(Event e, string source)
        {
            this.E = e;
            this.Source = source;
        }

        public override void Execute(RemoteEntity entity)
        {
            Broker b = (Broker)entity;
            List<string> interessedEntities = GetInteressedEntities(b, entity.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER));
            ProcessEventRouting((Broker)entity, interessedEntities);
        }

        //function that gets the interessed entities depending on the routing policy
        private List<string> GetInteressedEntities(Broker b, bool filter)
        {
            List<string> result = new List<string>();
            List<string> interessed = b.ForwardingTable.GetInterestedEntities(this.E.Topic);

            if (filter)
            {
                return interessed;
            }

            foreach (string item in interessed)
            {
                result.Add(item);
            }

            foreach(string item in b.Brokers.Keys.ToList())
            {
                if (!result.Contains(item))
                    result.Add(item);
            }

            return result;
        }


        //function to send the publish event to other brokers or subscribers
        private void ProcessEventRouting(Broker broker, List<string> interessedEntities)
        {
            bool entityFound = false;
            bool logDone = false;

            foreach (string entityName in interessedEntities)
            {
                entityFound = false;

                foreach (KeyValuePair<string, IRemoteBroker> entry in broker.Brokers)
                {
                    if (entry.Key.Equals(entityName))
                    {
                        if (!entry.Key.Equals(this.source))
                        {
                           
                            if (!logDone && broker.SysConfig.LogLevel.Equals(SysConfig.FULL))
                            {
                                broker.PuppetMaster.LogEventForwarding(broker.Name, this.E.Publisher, this.E.Topic, this.E.EventNr);
                                logDone = true;
                            }

                            entry.Value.DifundPublishEvent(this.E, broker.Name);
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
                        entry.Value.NotifyEvent(this.E);
                    }
                }
            }
        }
    }

    class DifundSubscribeEventCommand : Command
    {
        #region "Properties"
        private String topic;
        private string source;

        public string Topic
        {
            get
            {
                return topic;
            }

            set
            {
                topic = value;
            }
        }

        public string Source
        {
            get
            {
                return source;
            }

            set
            {
                source = value;
            }
        }
        #endregion

        public DifundSubscribeEventCommand(String topic, string source)
        {
            this.Topic = topic;
            this.Source = source;
        }

        public override void Execute(RemoteEntity entity)
        {
            Broker broker = (Broker) entity;
            List<string> iBrokers = broker.ReceiveTable.GetCreateTopicList(this.topic);
            broker.ForwardingTable.AddEntity(this.topic, this.source);

            //we only send the subscription to the brokers that we have not yet subscribed that topic to
            //if the request comes from a broker we do not subscribe that topic to him becouse thats pointless
            foreach (KeyValuePair<string, IRemoteBroker> entry in broker.Brokers)
            {
                if(!entry.Key.Equals(this.source) && !iBrokers.Contains(entry.Key.ToLower()))
                {
                    broker.ReceiveTable.AddTopic(this.Topic, entry.Key.ToLower());
                    entry.Value.DifundSubscribeEvent(this.Topic, broker.GetEntityName());
                }
            }

           
        }
    }

    class DifundUnSubscribeEventCommand : Command
    {
        #region "Properties"
        private String topic;
        private string source;

        public string TopicName
        {
            get
            {
                return topic;
            }

            set
            {
                topic = value;
            }
        }

        public string Source
        {
            get
            {
                return source;
            }

            set
            {
                source = value;
            }
        }
        #endregion

        public DifundUnSubscribeEventCommand(String topic, string source)
        {
            this.TopicName = topic;
            this.source = source;
        }

        public override void Execute(RemoteEntity entity)
        {
            Broker broker = (Broker)entity;
            List<string> topicsEl = Utils.GetTopicElements(this.topic);
            broker.ForwardingTable.RemoveEntity(this.topic, this.source);

            //check if we still have someone interested in that topic
            if (!(broker.ForwardingTable.GetInterestedEntities(this.TopicName).Count == 0))
                return;

            foreach (string brokerName in broker.ReceiveTable.GetCreateTopicList(this.topic))
            {
                broker.Brokers[brokerName].DifundUnSubscribeEvent(this.topic, broker.Name);
            }

            broker.ReceiveTable.RemoveTopic(this.topic);
        }

    }
}
