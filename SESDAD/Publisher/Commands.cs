using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;
using System.Threading;

namespace Publisher
{
    class PublishCommand : Command
    {
        private String topic;
        private int nrEvents;
        private int ms;

        public PublishCommand(String topic, int nrEvents, int ms)
        {
            this.topic = topic;
            this.nrEvents = nrEvents;
            this.ms = ms;
        }

        public override void Execute(RemoteEntity entity)
        {
            Publisher publisher = (Publisher)entity;

            for (int i = 0; i < this.nrEvents; i++)
            {
                publisher.CheckFreeze();
                publisher.FManager.ExecuteEventPublication(this.topic);
                Thread.Sleep(this.ms);
            }
        }
    }

    class ForwardPublishCommand : Command
    {
        private Event e;
        private string targetSite;
        private int outSeqNumber;

        public ForwardPublishCommand(Event e, string targetSite, int outSeqNumber)
        {
            this.e = e;
            this.targetSite = targetSite;
            this.outSeqNumber = outSeqNumber;
        }


        public override void Execute(RemoteEntity entity)
        {
            Publisher pEntity = (Publisher)entity;
            PublisherFaultManager fManager = pEntity.FManager;
            TimeoutMonitor tMonitor = fManager.TMonitor;
            IRemoteBroker broker = fManager.ChooseBroker(this.targetSite, this.e.Publisher);
            string brokerName = pEntity.RemoteNetwork.GetBrokerName(broker);
            int timeoutID = tMonitor.NewActionPerformed(e, outSeqNumber, targetSite, brokerName);

            try { broker.DifundPublishEvent(e, entity.RemoteNetwork.SiteName, entity.Name, outSeqNumber, timeoutID); } catch (Exception) { /*ignore*/ }
        }

    }

    class InformNewEventCommand : Command
    {
        private string topic;
        private string publisher;
        private int eventNr;

        public InformNewEventCommand(string topic, string publisher, int eventNr)
        {
            this.topic = topic;
            this.publisher = publisher;
            this.eventNr = eventNr;
        }

        public override void Execute(RemoteEntity entity)
        {
            Publisher pEntity = (Publisher)entity;
            PublisherFaultManager fManager = pEntity.FManager;

            foreach (IRemoteBroker broker in pEntity.RemoteNetwork.InBrokersList)
            {
                string brokerName = pEntity.RemoteNetwork.GetBrokerName(broker);

                if (!fManager.IsDead(pEntity.RemoteNetwork.SiteName, brokerName))
                {
                    try { broker.NewEventPublished(topic, publisher, eventNr); } catch (Exception) { /*ignore*/ }
                }
            }
        }
    }
}