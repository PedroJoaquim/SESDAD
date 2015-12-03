using Shared_Library;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Publisher
{
    class PublisherFaultManager : FaultManager
    {
        private const int NUM_THREADS = 50;
        private const int QUEUE_SIZE = 200;

        private int currentEventNr;

        public PublisherFaultManager(Publisher entity) : base(entity, QUEUE_SIZE, NUM_THREADS)
        {
            currentEventNr = 1;
        }

        public void ExecuteEventPublication(string topic)
        {

            lock(this)
            {
                int eventNr = this.currentEventNr;
                Event newEvent = new Event(RemoteEntity.Name, topic, new DateTime().Ticks, eventNr);

                RemoteEntity.PuppetMaster.LogEventPublication(RemoteEntity.Name, newEvent.Topic, newEvent.EventNr); //remote call
                this.Events.Produce(new ForwardPublishCommand(newEvent, RemoteEntity.RemoteNetwork.SiteName, eventNr));
                Console.WriteLine(String.Format("[PUBLISH EVENT] Topic: {0} EventNr: {1}", newEvent.Topic, newEvent.EventNr));
                this.currentEventNr++;

                if(RemoteEntity.SysConfig.Ordering.Equals(SysConfig.TOTAL))
                    this.Events.Produce(new InformNewEventCommand(newEvent.Topic, RemoteEntity.Name, newEvent.EventNr));
            }
        }

        /*
         * Timeout related methods
         */

        public override void ActionACKReceived(int timeoutID, string entityName, string entitySite)
        {
            TMonitor.PostACK(timeoutID);
            ResetMissedACKs(entitySite, entityName);
        }

        public override void ActionTimedout(ActionProperties ap)
        {
            DifundPublishEventProperties p = (DifundPublishEventProperties) ap;
            IncMissedACKs(p.TargetSite, p.TargetEntity);
            this.Events.Produce(new ForwardPublishCommand(p.E, RemoteEntity.RemoteNetwork.SiteName, p.E.EventNr));
        } 
    }
}
