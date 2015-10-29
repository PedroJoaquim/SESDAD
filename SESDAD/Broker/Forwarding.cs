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
        private Topic general = new Topic(Topic.GENERAL); //base element

        //tries to get the topic and if the topic does not exists is created
        public Topic GetCreateTopic(string topicName)
        {
            Topic currentSubtopic = general;
            List<string> topicsEl = Utils.GetTopicElements(topicName);

            foreach (string subTopic in topicsEl)
            {
                currentSubtopic = currentSubtopic.AddSubTopic(subTopic);    
            }

            return currentSubtopic;
        }

        public void AddEntity(string topicName, string entityName)
        {
            Console.WriteLine(String.Format("{0}  --->  {1}", topicName, entityName));
            GetCreateTopic(topicName).AddRemoteEntity(entityName);
        }

        public void RemoveEntity(string topicName, string entityName)
        {
            GetCreateTopic(topicName).RemoveRemoteEntity(entityName);
        }

        public List<string> GetInterestedEntities(string topicName)
        {
            return GetCreateTopic(topicName).Subscribers;
        }
    }

    //has the subscriptions send to other brokers
    class ReceiveTable
    {
        private Dictionary<string, List<string>> topicsSubscribed = new Dictionary<string, List<string>>();

        public void AddTopic(string topic, string broker)
        {
            List<string> brokers = GetCreateTopicList(topic);

            if (!brokers.Contains(broker))
                brokers.Add(broker);
        }

        public void RemoveTopic(string topic)
        {
            this.topicsSubscribed.Remove(topic);
        }

        //checks if the given topic is already was subscribed to all brokers
        public bool HasTopic(string topic)
        {
            return this.topicsSubscribed.ContainsKey(topic);
        }


        public List<string> GetCreateTopicList(string topic)
        {
            if(!this.topicsSubscribed.ContainsKey(topic))
            {
                this.topicsSubscribed[topic] = new List<string>();
            }

            return this.topicsSubscribed[topic];
        }

    }


    class Topic
    {
        public const string GENERAL = @"[b]";
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

        public Dictionary<string, Topic> SubTopics
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
            this.TopicName = topicName.ToLower();
        }

        public Topic GetSubTopic(string topicName)
        {
            Topic topic;
            string topicName2 = topicName.ToLower();
            return this.SubTopics.TryGetValue(topicName2, out topic) ? topic : null;
        }

        public Topic AddSubTopic(string topic)
        {
            string topicName = topic.ToLower();
            Topic subTopic = GetSubTopic(topicName);

            if(subTopic == null)
            {
                subTopic = new Topic(topicName);
                this.SubTopics.Add(topicName, subTopic);
            }

            return subTopic;           
        }

        public void RemoveSubtopic(string topicName)
        {
            string topicName2 = topicName.ToLower();
            this.SubTopics.Remove(topicName2);
        }

        public void AddRemoteEntity(string entityName)
        {
            string entityName2 = entityName.ToLower();
            if(!this.Subscribers.Contains(entityName2)) this.Subscribers.Add(entityName2);
        }

        public void RemoveRemoteEntity(string entityName)
        {
            string entityName2 = entityName.ToLower();
            if(this.Subscribers.Contains(entityName2)) this.Subscribers.Remove(entityName2);
        }
    }
}
