using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shared_Library
{

    [Serializable()]
    public class SysConfig : ISerializable
    {
        public const int PM_PORT = 30000;
        public const int PM_SLAVE_PORT = 30000;
        public const int REMOTE_CALL_TIMEOUT = 5000;
        public const String PM_NAME = "puppet";
        public const String PM_SLAVE_NAME = "puppet";
        public const String BROKER = "broker";
        public const String PUBLISHER = "publisher";
        public const String SUBSCRIBER = "subscriber";
        public const String FLOODING = "flooding";
        public const String FILTER = "filter";
        public const String LIGHT = "light";
        public const String FULL = "full";
        public const String FIFO = "fifo";
        public const String TOTAL = "total";
        public const String NO_ORDER = "no";


        #region "Attributes"
        private String logLevel = null;
        private String routingPolicy = null;
        private String ordering = null;
        private String distributed = null;
        private List<Connection> connections = null;

        #endregion

        #region "Properties"
        public string LogLevel
        {
            get
            {
                return logLevel;
            }

            set
            {
                logLevel = value;
            }
        }

        public string RoutingPolicy
        {
            get
            {
                return routingPolicy;
            }

            set
            {
                routingPolicy = value;
            }
        }

        public string Ordering
        {
            get
            {
                return ordering;
            }

            set
            {
                ordering = value;
            }
        }


        public string Distributed
        {
            get
            {
                return distributed;
            }

            set
            {
                distributed = value;
            }
        }

        public List<Connection> Connections
        {
            get
            {
                return connections;
            }

            set
            {
                connections = value;
            }
        }
        #endregion

        public SysConfig()
        {

        }

        public SysConfig cloneConfig()
        {
            SysConfig result = new SysConfig();

            result.LogLevel = this.LogLevel;
            result.RoutingPolicy = this.RoutingPolicy;
            result.Ordering = this.Ordering;
            result.Distributed = this.Distributed;
            result.connections = this.Connections;

            return result;
        }

        #region "Serialization"
        public SysConfig(SerializationInfo info, StreamingContext ctxt)
        {
            //Get the values from info and assign them to the appropriate properties
            logLevel = (String)info.GetValue("logLevel", typeof(String));
            routingPolicy = (String)info.GetValue("routingPolicy", typeof(String));
            ordering = (String)info.GetValue("ordering", typeof(String));
            connections = DeserializeConnections((String)info.GetValue("connections", typeof(String)));
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("logLevel", logLevel);
            info.AddValue("routingPolicy", routingPolicy);
            info.AddValue("ordering", ordering);
            info.AddValue("connections", serializeConnections());
        }

        private string serializeConnections()
        {
            String result = "";
            
            if (Connections != null)
            {
                foreach (Connection conn in Connections)
                {
                    result += conn.EntityName + "#" + conn.EntityURL + "#" + conn.EntitySite + "#" + conn.EntityType + "#";
                }
            }

            return result.Equals("") ? result : result.Remove(result.Length - 1);
        }

        private List<Connection> DeserializeConnections(String connStr)
        {
            List<Connection> result = new List<Connection>();

            if (!connStr.Equals(""))
            {
                string[] splitedConns = connStr.Split('#');
                for (int i = 0; i < splitedConns.Length - 1; i = i + 4)
                {
                    result.Add(new Connection(splitedConns[i], splitedConns[i+1], splitedConns[i + 2], splitedConns[i + 3]));
                }
            }

            return result;
        }
        #endregion
    }


    public class Connection
    {
        private string entityType;
        private string entityName;
        private string entitySite;
        private string entityURL;

        #region "properties"
        public string EntityType
        {
            get
            {
                return entityType;
            }

            set
            {
                entityType = value;
            }
        }

        public string EntityName
        {
            get
            {
                return entityName;
            }

            set
            {
                entityName = value;
            }
        }

        public string EntitySite
        {
            get
            {
                return entitySite;
            }

            set
            {
                entitySite = value;
            }
        }

        public string EntityURL
        {
            get
            {
                return entityURL;
            }

            set
            {
                entityURL = value;
            }
        }
        #endregion

        public Connection(string entityName, string entityURL, string entitySite, string entityType)
        {
            this.entityName = entityName;
            this.entityURL = entityURL;
            this.entitySite = entitySite;
            this.entityType = entityType;
        }
    }


    public class Utils
    {
        private static int START_INDEX = 6;

        public static string GetIPDomain(String url)
        {
            return url.Substring(START_INDEX, url.LastIndexOf(":") - START_INDEX);
        }

        public static string GetIPPort(String url)
        {
            int start = url.LastIndexOf(":") + 1;
            return url.Substring(start, url.LastIndexOf("/") - start);
        }

        public static string GetObjName(string myUrl)
        {
            return myUrl.Substring(myUrl.LastIndexOf("/") + 1);
        }

        public static List<string> GetTopicElements(string topic)
        {
            string[] tmp = topic.Trim().Split('/'); //test without the /
            List<string> result = new List<string>();

            foreach (string item in tmp)
            {
                if (!item.Trim().Equals(""))
                    result.Add(item);
            }

            return result;

        }

        public static List<T> MergeListsNoRepetitions<T>(List<T> l1, List<T> l2)
        {
            List<T> result = new List<T>();

            foreach (T item in l1)
            {
                if(!result.Contains(item))
                {
                    result.Add(item);
                }
            }

            foreach (T item in l2)
            {
                if (!result.Contains(item))
                {
                    result.Add(item);
                }
            }

            return result;
        }

        public static int CalcBrokerForwardIndex(int numBrokers, string sourcePublisherName, bool retransmission)
        {
            int hashCode = sourcePublisherName.GetHashCode();

            if (hashCode < 0)
                hashCode = hashCode * -1;

            return retransmission ? (hashCode + 1) % numBrokers : hashCode % numBrokers;
        }
    }

    public class Pair<T, U>
    {
        public Pair()
        {
        }

        public Pair(T first, U second)
        {
            this.First = first;
            this.Second = second;
        }

        public T First { get; set; }
        public U Second { get; set; }
    }

    [Serializable()]
    public class Event : ISerializable
    {
        private string publisher;
        private string topic;
        private long timestamp;
        private int eventNr;

        #region "Properties"
        public string Publisher
        {
            get
            {
                return publisher;
            }

            set
            {
                publisher = value;
            }
        }

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

        public long Timestamp
        {
            get
            {
                return timestamp;
            }

            set
            {
                timestamp = value;
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

        public Event(string publisher, string topic, long timestamp, int eventNr)
        {
            this.Publisher = publisher;
            this.Topic = topic;
            this.Timestamp = timestamp;
            this.EventNr = eventNr;
        }

        public Event(SerializationInfo info, StreamingContext ctxt)
        {
            publisher = (String)info.GetValue("publisher", typeof(String));
            topic = (String)info.GetValue("topic", typeof(String));
            timestamp = (long)info.GetValue("timestamp", typeof(long));
            eventNr = (int)info.GetValue("eventNr", typeof(int));
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("publisher", publisher);
            info.AddValue("topic", topic);
            info.AddValue("timestamp", timestamp);
            info.AddValue("eventNr", eventNr);
        }
    }
}
