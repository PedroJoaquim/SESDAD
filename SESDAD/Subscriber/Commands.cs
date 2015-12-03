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
                    try { entry.Value.DifundSubscribeEvent(topic, entity.Name); } 
                    catch (Exception) { /*ignore*/}
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
                    try { entry.Value.DifundUnSubscribeEvent(this.topic, entity.Name); } 
                    catch { /*ignore*/}
                  
                } catch (Exception)
                {
                    //ignore
                }
            }
        }
    }
}
