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
        #endregion

        public DifundPublishEventCommand(Event e, string sourceSite, string sourceEntity, int inSeqNumber)
        {
            this.e = e;
            this.sourceSite = sourceSite;
            this.sourceEntity = sourceEntity;
            this.inSeqNumber = inSeqNumber;
        }

        public override void Execute(RemoteEntity entity)
        {
            Broker b = (Broker) entity;
            b.PEventManager.ExecuteDistribution(this.sourceSite, this.sourceEntity, this.e, this.inSeqNumber);
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
                        try{ remoteBroker.DifundSubscribeEvent(this.topic, sourceSite); }
                        catch(Exception) { /*ignore*/}
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
                            try { remoteBroker.DifundUnSubscribeEvent(this.topic, sourceSite); } 
                            catch { /*ignore*/}
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
                            try { remoteBroker.DifundUnSubscribeEvent(this.topic, sourceSite); } 
                            catch { /*ignore*/}
  
                        }
                    }
                }

                broker.ReceiveTable.RemoveTopic(this.topic);
            }             
        }

    }

    class ReplicateCommand : Command
    {

        private IPassiveServer passiveServer;
        private Event e;
        private string sourceSite;
        private string sourceEntity;
        private int inSeqNumber;
        private int timeoutID;

        public ReplicateCommand(IPassiveServer passiveServer, Event e, string sourceSite, string sourceEntity, int inSeqNumber, int timeoutID)
        {
            this.passiveServer = passiveServer;
            this.e = e;
            this.sourceSite = sourceSite;
            this.sourceEntity = sourceEntity;
            this.inSeqNumber = inSeqNumber;
            this.timeoutID = timeoutID;
        }

        public override void Execute(RemoteEntity entity)
        {
            Broker broker = (Broker) entity;
            IRemoteEntity source;

            if (!broker.FManager.PassiveDead)
            {
                try
                {
                    passiveServer.StoreNewEvent(e, sourceSite, sourceEntity, inSeqNumber);
                } catch (Exception) { broker.FManager.PassiveDead = true; }
            }

            /*
             *  SEND ACK
             */
            if (sourceSite.Equals(entity.RemoteNetwork.SiteName))
                source = entity.RemoteNetwork.Publishers[sourceEntity];
            else
                source = entity.RemoteNetwork.OutBrokersNames[sourceEntity];

            try
            {
                source.ReceiveACK(timeoutID, entity.Name, entity.RemoteNetwork.SiteName);
            } catch (Exception) {}

        }
    }

    class EventDispatchedCommand : Command
    {

        private int eventNumber;
        private string publisher;
        private IPassiveServer passiveServer;

        public EventDispatchedCommand(int eventNumber, string publisher, IPassiveServer passiveServer)
        {
            this.eventNumber = eventNumber;
            this.passiveServer = passiveServer;
            this.publisher = publisher;
        }

        public override void Execute(RemoteEntity entity)
        {
            Broker broker = (Broker) entity;

            if (!broker.FManager.PassiveDead)
            {
                try
                {
                    this.passiveServer.EventDispatched(eventNumber, publisher);
                } catch (Exception) { broker.FManager.PassiveDead = true; }
            }
        }
    }

    class ForwardEventCommand : Command
    {
        private Event e;
        private string targetSite;
        private int outSeqNumber;
        private int actionID;

        public ForwardEventCommand(Event e, string targetSite, int outSeqNumber, int actionID)
        {
            this.e = e;
            this.targetSite = targetSite;
            this.outSeqNumber = outSeqNumber;
            this.actionID = actionID;
        }


        public override void Execute(RemoteEntity entity)
        {
            Broker bEntity = (Broker)entity;
            BrokerFaultManager fManager = bEntity.FManager;
            TimeoutMonitor tMonitor = fManager.TMonitor;
            IRemoteBroker broker = fManager.ChooseBroker(this.targetSite, this.e.Publisher);
            string brokerName = bEntity.RemoteNetwork.GetBrokerName(broker);
            int timeoutID = tMonitor.NewActionPerformed(e, outSeqNumber, targetSite, brokerName);

            fManager.RegisterNewTimeoutID(timeoutID, actionID);

            try { broker.DifundPublishEvent(e, entity.RemoteNetwork.SiteName, entity.Name, outSeqNumber, timeoutID);} 
            catch (Exception){ /*ignore*/ }

            Console.WriteLine(String.Format("[FORWARD EVENT] Topic: {0} Publisher: {1} EventNr: {2} To: {3}", e.Topic, e.Publisher, e.EventNr, entity.RemoteNetwork.GetBrokerName(broker)));
        }
    }

    class ForwardEventRetransmissionCommand : Command
    {
        private Event e;
        private string targetSite;
        private int outSeqNumber;
        private int oldTimeoutID;

        public ForwardEventRetransmissionCommand(Event e, string targetSite, int outSeqNumber, int oldTimeoutID)
        {
            this.e = e;
            this.targetSite = targetSite;
            this.outSeqNumber = outSeqNumber;
            this.oldTimeoutID = oldTimeoutID;
        }


        public override void Execute(RemoteEntity entity)
        {
            Broker bEntity = (Broker)entity;
            BrokerFaultManager fManager = bEntity.FManager;
            TimeoutMonitor tMonitor = fManager.TMonitor;
            IRemoteBroker broker = fManager.ChooseBroker(this.targetSite, this.e.Publisher);
            string brokerName = bEntity.RemoteNetwork.GetBrokerName(broker);

            tMonitor.NewActionPerformed(e, outSeqNumber, targetSite, brokerName, oldTimeoutID);

            try { broker.DifundPublishEvent(e, entity.RemoteNetwork.SiteName, entity.Name, outSeqNumber, oldTimeoutID); } 
            catch (Exception) { /*ignore*/ }

            Console.WriteLine(String.Format("[FORWARD EVENT RETRANSMISSION] Topic: {0} Publisher: {1} EventNr: {2} To: {3}", e.Topic, e.Publisher, e.EventNr, entity.RemoteNetwork.GetBrokerName(broker)));
        }
    }

    class TotalOrderNewEventCommand : Command
    {
        private string topic;
        private string publisher;
        private int eventNr;


        public TotalOrderNewEventCommand(string topic, string publisher, int eventNr)
        {
            this.topic = topic;
            this.publisher = publisher;
            this.eventNr = eventNr;
        }

        public override void Execute(RemoteEntity entity)
        {
            Broker broker = (Broker)entity;
            

            if(broker.SysConfig.ParentSite.Equals(SysConfig.NO_PARENT)) 
            {
                if (broker.IsSequencer || broker.IsPassiveSequencer) //current broker is the sequencer
                {
                    Event seqEvent = new Event(publisher, this.topic, new DateTime().Ticks, this.eventNr);
                    seqEvent.IsSequencerMessage = true;
                    string sourceEntity = broker.IsSequencer ? Sequencer.SEQUENCER_BASE_NAME : Sequencer.SEQUENCER_PASSIVE_NAME;

                    broker.PEventManager.ExecuteDistribution(broker.RemoteNetwork.SiteName, Sequencer.SEQUENCER_BASE_NAME, seqEvent, broker.Sequencer.GetNextSeqNumber());
                    
     
                }
            }
            else
            {
                List<IRemoteBroker> parentBrokers = broker.RemoteNetwork.OutBrokers[broker.SysConfig.ParentSite];

                foreach (IRemoteBroker parentBroker in parentBrokers)
                {
                    string brokerName = broker.RemoteNetwork.GetBrokerName(parentBroker);

                    if (!broker.FManager.IsDead(broker.SysConfig.ParentSite, brokerName))
                    {
                        try
                        {
                            parentBroker.NewEventPublished(this.topic, this.publisher, this.eventNr);
                        }
                        catch(Exception) { broker.FManager.MarkAsDead(broker.SysConfig.ParentSite, brokerName); }
                    }
                }

                broker.Sequencer.DispatchedNewEventMessage(publisher, eventNr, topic);
            }
        }
    }
}
