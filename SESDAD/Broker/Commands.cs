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
            Broker broker = (Broker) entity;
            bool entityFound;

            foreach (string entityName in broker.ForwardingTable.GetInterestedEntities(this.E.Topic))
            {
                entityFound = false;

                foreach(KeyValuePair<string, IRemoteBroker> entry in broker.Brokers)
                {
                    if (entry.Key.Equals(entityName))
                    {
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
            List<string> topicsEl = Utils.GetTopicElements(this.topic);
            Dictionary<string, Topic> currentTopicList = broker.Topics;
            Topic currentSubtopic = null;

            foreach (string subTopic in topicsEl)
            {
                if(currentTopicList.TryGetValue(subTopic, out currentSubtopic))
                {
                    currentTopicList = currentSubtopic.SubTopics;
                }
                else
                {
                    currentSubtopic = new Topic(subTopic);
                    currentTopicList.Add(subTopic, currentSubtopic);
                }
            }

            currentSubtopic.AddRemoteEntity(this.source);

            //TODO ... difund the event 


        }
    }

    class DifundUnSubscribeEventCommand : Command
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

        public DifundUnSubscribeEventCommand(String topic, string source)
        {
            this.Topic = topic;
            this.source = source;
        }

        public override void Execute(RemoteEntity entity)
        {
            throw new NotImplementedException();
        }
    }
}
