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
            bool entityFound;

            foreach (string entityName in interessedEntities)
            {
                entityFound = false;

                foreach (KeyValuePair<string, IRemoteBroker> entry in broker.Brokers)
                {
                    if (entry.Key.Equals(entityName))
                    {
                        if (!entry.Key.Equals(this.source))
                            entry.Value.DifundPublishEvent(this.E, broker.Name);

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
            Broker broker = (Broker)entity;
            broker.ForwardingTable.AddEntity(this.topic, this.source);

            //check if is necessary to send the subscription event to other brokers
            if (broker.ReceiveTable.HasTopic(this.Topic))
                return;

            foreach (KeyValuePair<string, IRemoteBroker> entry in broker.Brokers)
            {
                if(!entry.Key.Equals(this.source))
                {
                    entry.Value.DifundSubscribeEvent(this.Topic, broker.GetEntityName());
                }
            }

            broker.ReceiveTable.AddTopic(this.Topic);
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

            foreach (KeyValuePair<string, IRemoteBroker> entry in broker.Brokers)
            {
                if (!entry.Key.Equals(this.source))
                    entry.Value.DifundUnSubscribeEvent(this.topic, broker.Name);
            }

            broker.ReceiveTable.RemoveTopic(this.topic);
        }

    }
}
