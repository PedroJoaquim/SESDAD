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
        private string sourceSite;
        private string sourceEntity;
        private int inSeqNumber;

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
    
        public int InSeqNumber
        {
            get
            {
                return inSeqNumber;
            }

            set
            {
                inSeqNumber = value;
            }
        }

        public string SourceSite
        {
            get
            {
                return sourceSite;
            }

            set
            {
                sourceSite = value;
            }
        }

        public string SourceEntity
        {
            get
            {
                return sourceEntity;
            }

            set
            {
                sourceEntity = value;
            }
        }

        #endregion

        public DifundPublishEventCommand(Event e, string sourceSite, int inSeqNumber)
        {
            this.E = e;
            this.SourceSite = sourceSite;
            this.InSeqNumber = inSeqNumber;
        }

        public override void Execute(RemoteEntity entity)
        {
            Broker b = (Broker) entity;
            b.PEventManager.ExecuteDistribution(b, this.sourceSite, this.E, this.InSeqNumber);
            //TODO - fazer replicacao passiva para o outro broker
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

            if (!broker.ForwardingTable.TryAddEntity(this.topic, this.source))
                return; //already processed 

            ;

            //if the rout policy is flooding we do not need to send the sub event to other brokers
            if (entity.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER))
                ProcessFilteredDelivery(broker);

        }

        public void ProcessFilteredDelivery(Broker broker)
        {
            //we only send the subscription to the brokers that we have not yet subscribed that topic to
            //if the request comes from a broker we do not subscribe that topic to him becouse thats pointless

            string sourceSite = broker.RemoteNetwork.SiteName;

            foreach (KeyValuePair<string, List<IRemoteBroker>> entry in broker.RemoteNetwork.OutBrokers)
            {
                if (!entry.Key.Equals(this.source) && !broker.ReceiveTable.IsSubscribedTo(this.topic, entry.Key))
                {
                    broker.ReceiveTable.AddTopic(this.Topic, entry.Key.ToLower());

                    foreach (IRemoteBroker remoteBroker in entry.Value)
                    {
                        remoteBroker.DifundSubscribeEvent(this.Topic, sourceSite);
                    }
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

            if (!broker.ForwardingTable.TryRemoveEntity(this.topic, this.source))
                return; //already processed 

            

            //if the routing policy is flooding we do not need to send the unsub event to other brokers
            if (entity.SysConfig.RoutingPolicy.Equals(SysConfig.FILTER))
                ProcessFilteredDelivery(broker);
        }


        public void ProcessFilteredDelivery(Broker broker)
        {
            List<string> entitiesInterested = broker.ForwardingTable.GetInterestedEntities(this.TopicName);
            int entitiesInterestedCount = entitiesInterested.Count;
            string sourceSite = broker.RemoteNetwork.SiteName;

            //corner case
            //if the single interested entity is a site and we subscribed that topic to him we have to unsubscribe it
            //because they would forward the event to us and we back to them again

            if (entitiesInterestedCount == 1)
            {
                if(broker.ReceiveTable.IsSubscribedTo(this.topic, entitiesInterested[0]))
                {
                    if(broker.RemoteNetwork.GetAllOutSites().Contains(entitiesInterested[0]))
                    {
                        foreach (IRemoteBroker remoteBroker in broker.RemoteNetwork.OutBrokers[entitiesInterested[0]])
                        {
                            remoteBroker.DifundUnSubscribeEvent(this.topic, sourceSite);
                        }
                        
                        broker.ReceiveTable.RemoveEntityFromTopic(this.topic, entitiesInterested[0]);
                    }
                }
                    
            }

            //check if we still have someone interested in that topic
            //if not send unsubscribe event to all sites forwarding that event to us
            if (entitiesInterestedCount == 0)
            {
                foreach (string siteName in broker.ReceiveTable.GetCreateTopicList(this.topic))
                {
                    if(!siteName.Equals(this.source))
                    {
                        foreach (IRemoteBroker remoteBroker in broker.RemoteNetwork.OutBrokers[siteName])
                        {
                            remoteBroker.DifundUnSubscribeEvent(this.topic, sourceSite);
                        }
                    }
                }

                broker.ReceiveTable.RemoveTopic(this.topic);
            }             
        }

    }
}
