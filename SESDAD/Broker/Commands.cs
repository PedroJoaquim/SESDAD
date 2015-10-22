using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;

namespace Broker
{
    class DifundPublishEventCommand : Command
    {
        #region "Properties"
        private Event e;
        private bool source;

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

        public bool Source
        {
            get
            {
                return source;
            }

            set
            {
                source = value;
            }
        }
        #endregion

        public DifundPublishEventCommand(Event e, bool source)
        {
            this.E = e;
            this.source = source;
        }

        public override void Execute(RemoteEntity entity)
        {
            throw new NotImplementedException();
        }
    }

    class DifundSubscribeEventCommand : Command
    {
        #region "Properties"
        private String topic;
        private bool source;

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

        public bool Source
        {
            get
            {
                return source;
            }

            set
            {
                source = value;
            }
        }
        #endregion

        public DifundSubscribeEventCommand(String topic, bool source)
        {
            this.Topic = topic;
            this.Source = source;
        }

        public override void Execute(RemoteEntity entity)
        {
            Console.WriteLine(this.topic);
        }
    }

    class DifundUnSubscribeEventCommand : Command
    {
        #region "Properties"
        private String topic;
        private bool source;

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

        public bool Source
        {
            get
            {
                return source;
            }

            set
            {
                source = value;
            }
        }
        #endregion

        public DifundUnSubscribeEventCommand(String topic, bool source)
        {
            this.Topic = topic;
            this.source = source;
        }

        public override void Execute(RemoteEntity entity)
        {
            throw new NotImplementedException();
        }
    }
}
