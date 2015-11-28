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
        private int topicRelativeEventNr;

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

        public int TopicRelativeEventNr
        {
            get
            {
                return topicRelativeEventNr;
            }

            set
            {
                topicRelativeEventNr = value;
            }
        }
        #endregion

        public PublishCommand(String topic, int nrEvents, int ms)
        {
            this.Topic = topic;
            this.NrEvents = nrEvents;
            this.Ms = ms;
        }

        public override void Execute(RemoteEntity entity)
        {
            Publisher publisher = (Publisher) entity;

            for (int i = 0; i < this.nrEvents; i++)
            {
                publisher.CheckFreeze();
                publisher.FManager.ExecuteEventPublication(this.topic);
                Thread.Sleep(this.Ms);
            }
        }
    }
    
}
