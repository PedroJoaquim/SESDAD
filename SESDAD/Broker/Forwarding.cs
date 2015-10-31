using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;
using System.Runtime.CompilerServices;

namespace Broker
{
    class ForwardingTable
    {
        private Topic general = new Topic(Topic.GENERAL, null); //base element

        //tries to get the topic and if the topic does not exists is created
        private Topic GetCreateTopic(string topicName)
        {

            Topic currentSubtopic = general;
            List<string> topicsEl = Utils.GetTopicElements(topicName);

            foreach (string subTopic in topicsEl)
            {
                currentSubtopic = currentSubtopic.AddSubTopic(subTopic);
            }

            return currentSubtopic;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddEntity(string topicName, string entityName)
        {
            Console.WriteLine(String.Format("{0}  --->  {1}", topicName, entityName));
            GetCreateTopic(topicName).AddRemoteEntity(entityName);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void RemoveEntity(string topicName, string entityName)
        {
            GetCreateTopic(topicName).RemoveRemoteEntity(entityName);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<string> GetInterestedEntities(string topicName)
        {
            return GetCreateTopic(topicName).Subscribers;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<string> GetAllInterestedEntities(string topicName)
        {
            List<string> result = new List<string>();
            Topic currentTopic = GetCreateTopic(topicName);

            result = Utils.MergeListsNoRepetitions(result, currentTopic.Subscribers);

            if (currentTopic.GetSubTopic("*") != null)
            {
                result = Utils.MergeListsNoRepetitions(result, currentTopic.GetSubTopic("*").Subscribers);
            }

            while (currentTopic.Parent != null)
            {
                currentTopic = currentTopic.Parent;
                if (currentTopic.GetSubTopic("*") != null)
                {
                    result = Utils.MergeListsNoRepetitions(result, currentTopic.GetSubTopic("*").Subscribers);
                }
            }

            return result;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void PrintStatus()
        {
            foreach (KeyValuePair<string, Topic> entry in general.SubTopics)
            {
                entry.Value.Print();
            }
        }

        
    }

    //has the subscriptions send to other brokers
    class ReceiveTable
    {
        private Dictionary<string, List<string>> topicsSubscribed = new Dictionary<string, List<string>>();

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddTopic(string topic, string broker)
        {
            List<string> brokers = GetCreateTopicList(topic);

            if (!brokers.Contains(broker.ToLower()))
                brokers.Add(broker.ToLower());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void RemoveTopic(string topic)
        {
            this.topicsSubscribed.Remove(topic.ToLower());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void RemoveEntityFromTopic(string topic, string entity)
        {
            GetCreateTopicList(topic.ToLower()).Remove(entity.ToLower());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        //checks if the given topic is already was subscribed to all brokers
        public bool HasTopic(string topic)
        {
            return this.topicsSubscribed.ContainsKey(topic.ToLower());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<string> GetCreateTopicList(string topic)
        {
            if(!this.topicsSubscribed.ContainsKey(topic.ToLower()))
            {
                this.topicsSubscribed[topic.ToLower()] = new List<string>();
            }

            return this.topicsSubscribed[topic.ToLower()];
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        //checks if the we have already subscribed that topic to that entity
        public bool IsSubscribedTo(string topic, string entity)
        {
            return GetCreateTopicList(topic.ToLower()).Contains(entity.ToLower());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void PrintStatus()
        {
            foreach (KeyValuePair<string, List<string>> entry in topicsSubscribed)
            {
                foreach (string entity in entry.Value)
                {
                    Console.WriteLine("{0} ----------> {1}", entity, entry.Value);
                    Console.WriteLine();
                }
            }
        }
    }


    class Topic
    {
        public const string GENERAL = @"[b]";
        private string topicName;
        private Dictionary<string, Topic> subTopics = new Dictionary<string, Topic>();
        private List<string> subscribers = new List<string>();
        private Topic parent;
        private string fullName;

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

        internal Topic Parent
        {
            get
            {
                return parent;
            }

            set
            {
                parent = value;
            }
        }
        #endregion

        public Topic(string topicName, Topic parent)
        {
            this.TopicName = topicName.ToLower();
            this.parent = parent;

            if(parent == null)
            {
                this.fullName = "/";
            }
            else
            {
                this.fullName = parent.fullName + "/" + topicName;
            }
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
                subTopic = new Topic(topicName, this);
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

        public void Print()
        {
            string spaces = new string(' ', this.fullName.Length + 13);
            int i = 0;

            Console.WriteLine();

            foreach (string entity in this.Subscribers)
            {
                Console.WriteLine(String.Format("{0} ----------> {1}", i==0 ? fullName : spaces, entity));
                i++;
            }

            foreach (KeyValuePair<string, Topic> entry in this.SubTopics)
            {
                entry.Value.Print();
            }
        }
    }
}
