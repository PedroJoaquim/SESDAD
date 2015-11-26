using Shared_Library;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Publisher
{
    class FaultManager
    {
        private Publisher entity;

        public FaultManager(Publisher entity)
        {
            this.entity = entity;
        }

        public void ExecuteEventPublication(string topic)
        {
            entity.CheckFreeze();
            /*
            int eventNr = entity.ReadAndIncEventNr();
            Event newEvent = new Event(entity.Name, topic, new DateTime().Ticks, entity.ReadAndIncEventNr);
            this.PuppetMaster.LogEventPublication(this.Name, newEvent.Topic, newEvent.EventNr); //remote call
            int timeoutID = this.TMonitor.NewActionPerformed(newEvent, newEvent.EventNr, RemoteNetwork.SiteName);
            this.RemoteNetwork.ChooseBroker(RemoteNetwork.SiteName, Name, false).DifundPublishEvent(newEvent, RemoteNetwork.SiteName, this.Name, newEvent.EventNr, timeoutID); // remote call TODO CHANGEME
            Console.WriteLine("[EVENT] " + topic + " #" + this.CurrentEventNr);
            this.CurrentEventNr++;*/ //TODO

        }




    }
}
