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
        private const int NUM_THREADS = 15;
        private const int QUEUE_SIZE = 100;

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

                int timeoutID = this.TMonitor.NewActionPerformed(newEvent, newEvent.EventNr, RemoteEntity.RemoteNetwork.SiteName);
                bool retr = HasMissedMaxACKs(RemoteEntity.RemoteNetwork.SiteName);
                ExecuteEventTransmissionAsync(newEvent, RemoteEntity.RemoteNetwork.SiteName, newEvent.EventNr, timeoutID, retr);

                this.currentEventNr++;
            }
        }

        /*
         * Timeout related methods
         */

        public override void ActionACKReceived(int timeoutID, string entityName, string entitySite)
        {
            TMonitor.PostACK(timeoutID);
            ResetMissedACKs(RemoteEntity.RemoteNetwork.SiteName);
        }

        public override void ActionTimedout(DifundPublishEventProperties p)
        {
            int timeoutID = this.TMonitor.NewActionPerformed(p.E, p.E.EventNr, RemoteEntity.RemoteNetwork.SiteName, p.Id);
            bool retr = HasMissedMaxACKs(RemoteEntity.RemoteNetwork.SiteName);

            IncMissedACKs(RemoteEntity.RemoteNetwork.SiteName);

            ExecuteEventTransmissionAsync(p.E, p.TargetSite, p.E.EventNr, timeoutID, retr);
        } 
    }
}
