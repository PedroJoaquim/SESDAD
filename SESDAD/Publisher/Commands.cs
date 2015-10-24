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
        #region "properties"
        private String topic;
        private int nrEvents;
        private int ms;
        private int eventNr;

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

        public int NrEvents
        {
            get
            {
                return nrEvents;
            }

            set
            {
                nrEvents = value;
            }
        }

        public int Ms
        {
            get
            {
                return ms;
            }

            set
            {
                ms = value;
            }
        }

        public int EventNr
        {
            get
            {
                return eventNr;
            }

            set
            {
                eventNr = value;
            }
        }
        #endregion

        public PublishCommand(String topic, int nrEvents, int ms, int eventNr)
        {
            this.Topic = topic;
            this.NrEvents = nrEvents;
            this.Ms = ms;
            this.EventNr = eventNr;
        }

        public override void Execute(RemoteEntity entity)
        {
            IRemoteBroker broker = entity.Brokers.ElementAt(0).Value; //first broker
            Event newEvent;

            for (int i = 0; i < this.nrEvents; i++)
            {
                newEvent = new Event(entity.Name, this.Topic, new DateTime().Ticks, this.EventNr + i);
                broker.DifundPublishEvent(newEvent, true); // remote call

                entity.PuppetMaster.LogEventPublication(entity.Name, newEvent.Topic, newEvent.EventNr);

                Thread.Sleep(this.Ms);
            }
             

        }
    }
}
