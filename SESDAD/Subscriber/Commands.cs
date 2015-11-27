using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;

namespace Subscriber
{
    class SubscribeCommand : Command
    {
        #region "properties"
        private String topic;

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
        #endregion

        public SubscribeCommand(String topic)
        {
            this.Topic = topic;
        }

        public override void Execute(RemoteEntity entity)
        {
            foreach (KeyValuePair<string, IRemoteBroker> entry in entity.RemoteNetwork.InBrokers)
            {
                try
                {
                    entry.Value.DifundSubscribeEvent(topic, entity.Name); //remote call
                }
                catch(Exception)
                {
                    //ignore
                }   
            }
        }
    }

    class UnsubscribeCommand : Command
    {
        #region "properties"
        private String topic;

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
        #endregion

        public UnsubscribeCommand(String topic)
        {
            this.Topic = topic;
        }


        public override void Execute(RemoteEntity entity)
        {
            foreach (KeyValuePair<string, IRemoteBroker> entry in entity.RemoteNetwork.InBrokers)
            {
                try
                {
                    entry.Value.DifundUnSubscribeEvent(this.topic, entity.Name);
                } catch (Exception)
                {
                    //ignore
                }
            }
        }
    }

    class NotifyEvent : Command
    {
        #region "Properties"
        private Event e;

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
        #endregion

        public NotifyEvent(Event e)
        {
            this.E = e;
        }

        public override void Execute(RemoteEntity entity)
        {
            Subscriber s = (Subscriber)entity;

            if(s.ValidEvent(this.E))
            {
                entity.PuppetMaster.LogEventDelivery(entity.Name, this.E.Publisher, this.E.Topic, this.E.EventNr);
                Console.WriteLine(String.Format("[EVENT {3}] {1} -----> {0} #{2}", this.E.Topic, this.E.Publisher, this.E.EventNr, entity.Name));
            }
        }
    }
}
