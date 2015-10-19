using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;

namespace Publisher
{
    class PublishCommand : Command
    {
        #region "properties"
        private String topic;
        private int nrEvents;
        private int ms;

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
        #endregion

        public PublishCommand(String topic, int nrEvents, int ms)
        {
            this.Topic = topic;
            this.NrEvents = nrEvents;
            this.Ms = ms;
        }

        public override void Execute(IRemoteEntity entity)
        {
            throw new NotImplementedException();
        }
    }
}
