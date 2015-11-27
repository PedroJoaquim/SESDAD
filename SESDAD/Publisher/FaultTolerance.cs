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
        private const int MAX_MISSED_ACKS = 10;

        private int missedACKs;
        private int currentEventNr;
        private Object acksLock;

        public PublisherFaultManager(Publisher entity) : base(entity)
        {
            missedACKs = 0;
            currentEventNr = 1;
            acksLock = new Object();
        }

        public void ExecuteEventPublication(string topic)
        {

            lock(this)
            {
                int eventNr = this.currentEventNr;
                Event newEvent = new Event(RemoteEntity.Name, topic, new DateTime().Ticks, eventNr);
                RemoteEntity.PuppetMaster.LogEventPublication(RemoteEntity.Name, newEvent.Topic, newEvent.EventNr); //remote call
                int timeoutID = this.TMonitor.NewActionPerformed(newEvent, newEvent.EventNr, RemoteEntity.RemoteNetwork.SiteName);
                bool retr;

                lock(acksLock)
                {
                    retr = missedACKs >= MAX_MISSED_ACKS;
                }

                ExecuteEventTransmissionAsync(newEvent, RemoteEntity.RemoteNetwork.SiteName, newEvent.EventNr, timeoutID, retr);
                Console.WriteLine("[EVENT] " + topic + " #" + this.currentEventNr);
                this.currentEventNr++;
            }
        }

        /*
         * Timeout related methods
         */

        public override void ActionACKReceived(int timeoutID, string entityName)
        {
            this.TMonitor.PostACK(timeoutID);

            lock(acksLock)
            {
                if (missedACKs > MAX_MISSED_ACKS) //ignore
                    return;
                else
                    missedACKs = 0;
            }
        }

        public override void ActionTimedout(DifundPublishEventProperties p)
        {
            RemoteEntity.CheckFreeze();

            int timeoutID = this.TMonitor.NewActionPerformed(p.E, p.E.EventNr, RemoteEntity.RemoteNetwork.SiteName);
            ExecuteEventTransmissionAsync(p.E, p.TargetSite, p.E.EventNr, timeoutID, true);
            Console.WriteLine("[EVENT] - #" + p.E.EventNr + " RETRANSMITED ");

            lock (acksLock)
            {
                missedACKs++;
            }
        } 
    }
}
