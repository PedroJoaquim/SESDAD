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
        private int currentEventNr;

        public PublisherFaultManager(Publisher entity) : base(entity)
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
                string brokerName = ExecuteEventTransmissionAsync(newEvent, RemoteEntity.RemoteNetwork.SiteName, newEvent.EventNr, timeoutID, retr);

                Console.WriteLine("[EVENT] " + topic + " #" + this.currentEventNr + " SENT TO: " + brokerName);
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
            RemoteEntity.CheckFreeze();

            int timeoutID = this.TMonitor.NewActionPerformed(p.E, p.E.EventNr, RemoteEntity.RemoteNetwork.SiteName, p.Id);
            bool retr = HasMissedMaxACKs(RemoteEntity.RemoteNetwork.SiteName);
            IncMissedACKs(RemoteEntity.RemoteNetwork.SiteName);

            string brokerName = ExecuteEventTransmissionAsync(p.E, p.TargetSite, p.E.EventNr, timeoutID, retr);
            Console.WriteLine("[EVENT] - #" + p.E.EventNr + " RETRANSMITED TO: " + brokerName);
        } 
    }
}
