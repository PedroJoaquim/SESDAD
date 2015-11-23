using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shared_Library
{
    public class RemoteNetwork
    {
        private string siteName;
        private Dictionary<string, Dictionary<string, IRemoteBroker>> outBrokers = new Dictionary<string, Dictionary<string, IRemoteBroker>>();
        private Dictionary<string, IRemoteBroker> inBrokers = new Dictionary<string, IRemoteBroker>();
        private Dictionary<string, IRemotePublisher> publishers = new Dictionary<string, IRemotePublisher>();
        private Dictionary<string, IRemoteSubscriber> subscribers = new Dictionary<string, IRemoteSubscriber>();

        #region "properties"
        public string SiteName
        {
            get
            {
                return siteName;
            }

            set
            {
                siteName = value;
            }
        }

        public Dictionary<string, IRemoteBroker> InBrokers
        {
            get
            {
                return inBrokers;
            }

            set
            {
                inBrokers = value;
            }
        }

        public Dictionary<string, IRemotePublisher> Publishers
        {
            get
            {
                return publishers;
            }

            set
            {
                publishers = value;
            }
        }

        public Dictionary<string, IRemoteSubscriber> Subscribers
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

        public Dictionary<string, Dictionary<string, IRemoteBroker>> OutBrokers
        {
            get
            {
                return outBrokers;
            }

            set
            {
                outBrokers = value;
            }
        }

        public Dictionary<string, IRemoteBroker> GetOutBrokers(string siteName)
        {
            if (OutBrokers.ContainsKey(siteName))
                return OutBrokers[siteName];
            else
                return null;
        }

        public Dictionary<string, IRemoteBroker> GetAllOutBrokers()
        {
            Dictionary<string, IRemoteBroker> result = new Dictionary<string, IRemoteBroker>();

            foreach (KeyValuePair<string, Dictionary<string, IRemoteBroker>> entry in OutBrokers)
            {
                foreach (KeyValuePair<string, IRemoteBroker> entry2 in entry.Value)
                {
                    result.Add(entry2.Key, entry2.Value);
                }
            }

            return result;
        }
        #endregion

        public void Initialize(SysConfig sysConfig)
        {
            foreach (Connection conn in sysConfig.Connections)
            {
                switch (conn.EntityType)
                {
                    case SysConfig.BROKER:
                        AddNewBrokerConnection(conn);                        
                        break;
                    case SysConfig.SUBSCRIBER:
                        IRemoteSubscriber newSubscriber = (IRemoteSubscriber)Activator.GetObject(typeof(IRemoteSubscriber), conn.EntityURL);
                        this.Subscribers.Add(conn.EntityName, newSubscriber);
                        break;
                    case SysConfig.PUBLISHER:
                        IRemotePublisher newPublisher = (IRemotePublisher)Activator.GetObject(typeof(IRemotePublisher), conn.EntityURL);
                        this.Publishers.Add(conn.EntityName, newPublisher);
                        break;
                    default:
                        break;
                }

                Console.WriteLine(String.Format("[INFO] {0} [{1}] added on: {2}", conn.EntityName, conn.EntityType, conn.EntityURL));
            }
        }

        private void AddNewBrokerConnection(Connection conn)
        {
            IRemoteBroker newBroker = (IRemoteBroker)Activator.GetObject(typeof(IRemoteBroker), conn.EntityURL);

            if (!conn.EntitySite.Equals(this.SiteName)) //outside brokers
            {
                if (!OutBrokers.ContainsKey(conn.EntitySite))
                {
                    OutBrokers.Add(conn.EntitySite, new Dictionary<string, IRemoteBroker>());
                }

                OutBrokers[conn.EntitySite].Add(conn.EntityName, newBroker);
            }
            else //inside broker
            {
                InBrokers.Add(conn.EntityName, newBroker);
            }
        }
    }
}
