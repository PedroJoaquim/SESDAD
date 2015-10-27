using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;

namespace Broker
{
    class ForwardingTable
    {
        private Dictionary<string, List<string>> links = new Dictionary<string, List<string>>();

        public void AddEntity(string topic, string entityName)
        {
            List<string> eList = null;

            this.links.TryGetValue(topic, out eList);

            if (eList == null)
            {
                eList = new List<string>();
                this.links.Add(topic, eList);
            }

            eList.Add(entityName);
        }

        public void RemoveEntity(string topic, string entityName)
        {
            List<string> eList = null;

            this.links.TryGetValue(topic, out eList);

            if(eList != null)
            {
                foreach (string entityName2 in eList)
                {
                    if (entityName2.Equals(entityName))
                        eList.Remove(entityName);
                }

                if(eList.Count == 0)
                {
                    this.links.Remove(topic);
                }
            }

        }

        public List<string> GetInterestedEntities(string topic)
        {
            List<string> result;
            return this.links.TryGetValue(topic, out result) ? result : new List<string>();
        }
    }

    class ReceiveTable
    {
        List<string> topics = new List<string>();

        public void AddTopic(string topic)
        {
            this.topics.Add(topic);
        }

        public void RemoveTopic(string topic)
        {
            foreach (string item in this.topics)
            {
                if (item.Equals(topic))
                    this.topics.Remove(topic);
            }
        }

        public bool HasTopic(string topic)
        {
            foreach (string item in this.topics)
            {
                if (Includes(item, topic))
                    return true;
            }

            return false;
        }

        //checks if topic2 is included in topic1
        private bool Includes(string topic1, string topic2)
        {
            List<string> topicEl1 = Utils.GetTopicElements(topic1);
            List<string> topicEl2 = Utils.GetTopicElements(topic2);
            
            for(int i = 0; i < topicEl1.Count && i < topicEl2.Count; i++)
            {
                if (topicEl1[i].Equals("*")) return true;
                if (!topicEl1[i].Equals(topicEl2[i])) return false;
            }

            return topicEl1.Count == topicEl2.Count; //may be necessary to change wait teacher answer

        }
    }


    class Topic
    {
        private string topicName;
        private Dictionary<string, Topic> subTopics = new Dictionary<string, Topic>();
        private List<string> subscribers = new List<string>();

        #region "properties"
        public string TopicName
        {
            get
            {
                return topicName;
            }

            set
            {
                topicName = value;
            }
        }

        internal Dictionary<string, Topic> SubTopics
        {
            get
            {
                return subTopics;
            }

            set
            {
                subTopics = value;
            }
        }

        public List<string> Subscribers
        {
            get
            {
                return subscribers;
            }

            set
            {
                subscribers = value;
            }
        }
        #endregion

        public Topic(string topicName)
        {
            this.TopicName = topicName;
        }

        public Topic GetSubTopic(string topicName)
        {
            Topic topic;
            return this.SubTopics.TryGetValue(topicName, out topic) ? topic : null;
        }

        public Topic AddSubTopic(string topicName)
        {
            Topic subTopic = GetSubTopic(topicName);

            if(subTopic == null)
            {
                subTopic = new Topic(topicName);
                this.SubTopics.Add(topicName, subTopic);
            }

            return subTopic;           
        }

        public void RemoveSubtopic(string topic)
        {
            this.SubTopics.Remove(topic);
        }

        public void AddRemoteEntity(string entity)
        {
            this.Subscribers.Add(entity);
        }

        public void RemoveRemoteEntity(string entityName)
        {
            this.Subscribers.Remove(entityName);
        }
    }
}
