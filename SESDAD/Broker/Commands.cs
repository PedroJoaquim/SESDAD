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
        private int inSeqNumber;
        private int timeoutID;
        #endregion

        public DifundPublishEventCommand(Event e, string sourceSite, int inSeqNumber, int timeoutID)
        {
            this.e = e;
            this.sourceSite = sourceSite;
            this.inSeqNumber = inSeqNumber;
            this.timeoutID = timeoutID;
        }

        public override void Execute(RemoteEntity entity)
        {
            Broker b = (Broker) entity;
            b.PEventManager.ExecuteDistribution(b, this.sourceSite, this.e, this.inSeqNumber);
            //TODO - fazer replicacao passiva para o outro broker
        }

    }

    class DifundSubscribeEventCommand : Command
    {
        #region "Properties"
        private String topic;
        private string source;
        #endregion

        public DifundSubscribeEventCommand(String topic, string source)
        {
            this.topic = topic;
            this.source = source;
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
                    broker.ReceiveTable.AddTopic(this.topic, entry.Key.ToLower());

                    foreach (IRemoteBroker remoteBroker in entry.Value)
                    {
                        remoteBroker.DifundSubscribeEvent(this.topic, sourceSite);
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
        #endregion

        public DifundUnSubscribeEventCommand(String topic, string source)
        {
            this.topic = topic;
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
            List<string> entitiesInterested = broker.ForwardingTable.GetInterestedEntities(this.topic);
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

    class SendACKCommand : Command
    {
        private int timeoutID;
        private string sourceEntity;
        private string sourceSite;

        public SendACKCommand(int timeoutID, string sourceEntity, string sourceSite)
        {
            this.timeoutID = timeoutID;
            this.sourceEntity = sourceEntity;
            this.sourceSite = sourceSite;
        }

        public override void Execute(RemoteEntity entity)
        {
            IRemoteEntity source;

            if (sourceSite.Equals(entity.RemoteNetwork.SiteName))
                source = entity.RemoteNetwork.Publishers[sourceEntity];
            else
                source = entity.RemoteNetwork.OutBrokersNames[sourceEntity];

            try
            {
                source.ReceiveACK(timeoutID, entity.Name, entity.RemoteNetwork.SiteName);
            } catch (Exception) {/*ignore*/}
        }
    }

    class EventDispatchedCommand : Command
    {

        private int eventNr;
        private string publisher;
        private IPassiveServer passiveServer;

        public EventDispatchedCommand(int eventNr, string publisher, IPassiveServer passiveServer)
        {
            this.eventNr = eventNr;
            this.publisher = publisher;
            this.passiveServer = passiveServer;
        }

        public override void Execute(RemoteEntity entity)
        {
            try
            {
                this.passiveServer.EventDispatched(eventNr, publisher);
            } catch (Exception) { /* ignore */ }
        }


    }
}
