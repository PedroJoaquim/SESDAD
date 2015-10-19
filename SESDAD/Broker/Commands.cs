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

        public DifundPublishEventCommand(String topic)
        {
            this.Topic = topic;
        }

        public override void Execute(IRemoteEntity entity)
        {
            throw new NotImplementedException();
        }
    }

    class DifundSubscribeEventCommand : Command
    {
        #region "Properties"
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

        public DifundSubscribeEventCommand(String topic)
        {
            this.Topic = topic;
        }

        public override void Execute(IRemoteEntity entity)
        {
            throw new NotImplementedException();
        }
    }

    class DifundUnSubscribeEventCommand : Command
    {
        #region "Properties"
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

        public DifundUnSubscribeEventCommand(String topic)
        {
            this.Topic = topic;
        }

        public override void Execute(IRemoteEntity entity)
        {
            throw new NotImplementedException();
        }
    }
}
