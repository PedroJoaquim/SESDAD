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
        private Dictionary<string, IRemoteBroker> outBrokersNames = new Dictionary<string, IRemoteBroker>();
        private Dictionary<string, List<IRemoteBroker>> outBrokers = new Dictionary<string, List<IRemoteBroker>>();
        private Dictionary<string, IRemoteBroker> inBrokers = new Dictionary<string, IRemoteBroker>();
        private List<IRemoteBroker> inBrokersList = new List<IRemoteBroker>();
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

        public Dictionary<string, List<IRemoteBroker>> OutBrokers
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

        public Dictionary<string, IRemoteBroker> OutBrokersNames
        {
            get
            {
                return outBrokersNames;
            }

            set
            {
                outBrokersNames = value;
            }
        }

        public List<IRemoteBroker> InBrokersList
        {
            get
            {
                return inBrokersList;
            }

            set
            {
                inBrokersList = value;
            }
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

            OrderBrokers();
        }

        private void OrderBrokers()
        {
            inBrokersList.Sort((x, y) => string.Compare(x.GetEntityName(), y.GetEntityName()));

            foreach (KeyValuePair<string, List<IRemoteBroker>> entry in outBrokers)
            {
                entry.Value.Sort((x, y) => string.Compare(x.GetEntityName(), y.GetEntityName()));
            }
        }

        public IRemoteBroker ChooseBroker(string site, string publisher, bool retransmission)
        {
            List<IRemoteBroker> brokers = site.Equals(siteName) ? InBrokersList : OutBrokers[site];
            int index = Utils.CalcBrokerForwardIndex(brokers.Count, publisher, retransmission);

            return brokers[index];
        }


        public List<string> GetAllOutSites()
        {
            List<string> result = new List<string>();

            foreach (KeyValuePair<string, List<IRemoteBroker>> item in outBrokers)
            {
                result.Add(item.Key);
            }

            return result;
        }


        private void AddNewBrokerConnection(Connection conn)
        {
            IRemoteBroker newBroker = (IRemoteBroker)Activator.GetObject(typeof(IRemoteBroker), conn.EntityURL);

            if (!conn.EntitySite.Equals(this.SiteName)) //outside brokers
            {
                if (!OutBrokers.ContainsKey(conn.EntitySite))
                {
                    OutBrokers.Add(conn.EntitySite, new List<IRemoteBroker>());
                }

                OutBrokers[conn.EntitySite].Add(newBroker);
                OutBrokersNames[conn.EntityName] = newBroker;
            }
            else //inside broker
            {
                InBrokers.Add(conn.EntityName, newBroker);
                InBrokersList.Add(newBroker);
            }
        }
    }
}
