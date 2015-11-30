using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;

namespace PuppetMaster
{

    class SystemNetwork
    {
        #region "Attributes"
        private SysConfig systemConfig = new SysConfig();
        private Dictionary<String, Entity> entities = new Dictionary<string, Entity>();
        private Dictionary<String, Site> siteMap = new Dictionary<string, Site>();
        #endregion


        public SystemNetwork()
        {
            //setting system default values
            this.SystemConfig.LogLevel = "light";
            this.SystemConfig.RoutingPolicy = "flooding";
            this.SystemConfig.Ordering = "fifo";
            this.SystemConfig.Distributed = "localhost";
        }


        #region "Properties"
        public string LogLevel
        {
            get
            {
                return this.SystemConfig.LogLevel;
            }

            set
            {
                this.SystemConfig.LogLevel = value;
            }
        }

        public string RoutingPolicy
        {
            get
            {
                return this.SystemConfig.RoutingPolicy;
            }

            set
            {
                this.SystemConfig.RoutingPolicy = value;
            }
        }

        public string Ordering
        {
            get
            {
                return this.SystemConfig.Ordering;
            }

            set
            {
                this.SystemConfig.Ordering = value;
            }
        }

        public string Distributed
        {
            get
            {
                return this.SystemConfig.Distributed;
            }

            set
            {
                this.SystemConfig.Distributed = value;
            }
        }

        public Dictionary<string, Entity> Entities
        {
            get
            {
                return entities;
            }

            set
            {
                entities = value;
            }
        }

        public Dictionary<string, Site> SiteMap
        {
            get
            {
                return siteMap;
            }

            set
            {
                siteMap = value;
            }
        }

        public SysConfig SystemConfig
        {
            get
            {
                return systemConfig;
            }

            set
            {
                systemConfig = value;
            }
        }
        #endregion

        public Site GetSite(String name)
        {
            Site target;
            return SiteMap.TryGetValue(name, out target) ? target : null;
        }

        public void AddSite(Site newSite)
        {
            SiteMap.Add(newSite.Name, newSite);
        }

        public Entity GetEntity(String name)
        {
            Entity target;
            return Entities.TryGetValue(name, out target) ? target : null;
        }

        public void AddEntity(Entity newEntity)
        {
            Entities.Add(newEntity.Name, newEntity);
        }
    }

    public class Site
    {

        private string name = null;
        private Site parent = null;
        private List<Site> children = new List<Site>();
        private Dictionary<String, BrokerEntity> brokerEntities = new Dictionary<string, BrokerEntity>();
        private Dictionary<String, PublisherEntity> publisherEntities = new Dictionary<string, PublisherEntity>();
        private Dictionary<String, SubscriberEntity> subscriberEntities = new Dictionary<string, SubscriberEntity>();


        #region "Properties"
        public Site Parent
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

        public string Name
        {
            get
            {
                return name;
            }

            set
            {
                name = value;
            }
        }

        public List<Site> Children
        {
            get
            {
                return children;
            }

            set
            {
                children = value;
            }
        }

        public Dictionary<string, BrokerEntity> BrokerEntities
        {
            get
            {
                return brokerEntities;
            }

            set
            {
                brokerEntities = value;
            }
        }

        public Dictionary<string, PublisherEntity> PublisherEntities
        {
            get
            {
                return publisherEntities;
            }

            set
            {
                publisherEntities = value;
            }
        }

        public Dictionary<string, SubscriberEntity> SubscriberEntities
        {
            get
            {
                return subscriberEntities;
            }

            set
            {
                subscriberEntities = value;
            }
        }
        #endregion

        public Site(String name)
        {
            this.name = name;
        }

        /*
         *  Function that get all broker's urls that have a connection to this site
         */
        public List<Connection> GetOutsideBrokersUrls(string ignoreCase = "")
        {
            List<Connection> connectionURLS = new List<Connection>();

            if (this.Parent != null)
            {
                connectionURLS.AddRange(this.Parent.GetBrokerUrls(ignoreCase));
            }

            foreach (Site currentSite in this.Children)
            {
                connectionURLS.AddRange(currentSite.GetBrokerUrls(ignoreCase));
            }

            return connectionURLS;
        }

        public List<Connection> GetPublisherUrls(string ignoreCase = "")
        {
            List<Connection> result = new List<Connection>();

            foreach (KeyValuePair<string, PublisherEntity> entry in this.PublisherEntities)
            {
                if (!entry.Value.Url.Equals(ignoreCase))
                {
                    result.Add(new Connection(entry.Key, entry.Value.Url, this.name, SysConfig.PUBLISHER));
                }
            }

            return result;
        }

        public List<Connection> GetSubscriberUrls(string ignoreCase = "")
        {
            List<Connection> result = new List<Connection>();

            foreach (KeyValuePair<string, SubscriberEntity> entry in this.SubscriberEntities)
            {
                if (!entry.Value.Url.Equals(ignoreCase))
                {
                    result.Add(new Connection(entry.Key, entry.Value.Url, this.name, SysConfig.SUBSCRIBER));
                }
            }

            return result;
        }

        public List<Connection> GetBrokerUrls(string ignoreCase = "")
        {
            List<Connection> result = new List<Connection>();

            foreach (KeyValuePair<string, BrokerEntity> entry in this.BrokerEntities)
            {
                if (!entry.Value.Url.Equals(ignoreCase))
                {
                    result.Add(new Connection(entry.Key, entry.Value.Url, this.name, SysConfig.BROKER));
                }
            }

            return result;
        }

        internal List<string> GetOrderedBrokerEntities()
        {
            List<string> result = new List<string>();

            foreach (KeyValuePair<string, BrokerEntity> item in this.BrokerEntities)
            {
                result.Add(item.Key);
            }

            result.Sort((x, y) => string.Compare(x, y));

            return result;
        }
    }


    public abstract class Entity
    {

        private Site site = null;
        private String name = null;
        private String url = null;

        #region "Properties"
        public Site Site
        {
            get
            {
                return site;
            }

            set
            {
                site = value;
            }
        }

        public string Name
        {
            get
            {
                return name;
            }

            set
            {
                name = value;
            }
        }

        public string Url
        {
            get
            {
                return url;
            }

            set
            {
                url = value;
            }
        }
        #endregion

        public Entity(String name, String url)
        {
            this.name = name;
            this.url = url;
        }

        public abstract List<Connection> GetConnectionsUrl();
        public abstract String EntityType();
        public abstract IRemoteEntity GetRemoteEntity();
    }

    public class BrokerEntity : Entity
    {
        private IRemoteBroker remoteEntity;

        public BrokerEntity(String name, String url) : base(name, url) { }

        #region "properties"
        public IRemoteBroker RemoteEntity
        {
            get
            {
                return remoteEntity;
            }

            set
            {
                remoteEntity = value;
            }
        }
        #endregion


        public override List<Connection> GetConnectionsUrl()
        {
            List<Connection> connectionURLS = new List<Connection>();

            //brokers have connections with other brokers in other sites and all publishers and subscribers in this site
            connectionURLS.AddRange(this.Site.GetOutsideBrokersUrls(this.Url));
            connectionURLS.AddRange(this.Site.GetPublisherUrls(this.Url));
            connectionURLS.AddRange(this.Site.GetSubscriberUrls(this.Url));
            connectionURLS.AddRange(this.Site.GetBrokerUrls(this.Url));

            return connectionURLS;
        }

        
        public override String EntityType()
        {
            return SysConfig.BROKER;
        }

        public override IRemoteEntity GetRemoteEntity()
        {
            return this.RemoteEntity;
        }

        internal string GetPassiveServer()
        {
            List<string> brokers = this.Site.GetOrderedBrokerEntities();

            for (int i = 0; i < brokers.Count; i++)
            {
                if (brokers[i].Equals(this.Name))
                    return brokers[(i + 1) % brokers.Count];
            }

            return "";

        }
    }


    public class PublisherEntity : Entity
    {
        private IRemotePublisher remoteEntity;

        public PublisherEntity(String name, String url) : base(name, url) { }

        #region "properties"
        public IRemotePublisher RemoteEntity
        {
            get
            {
                return remoteEntity;
            }

            set
            {
                remoteEntity = value;
            }
        }
        #endregion

        public override List<Connection> GetConnectionsUrl()
        {
            List<Connection> connectionURLS = new List<Connection>();

            connectionURLS.AddRange(this.Site.GetBrokerUrls(this.Url));

            return connectionURLS;
        }


        public override String EntityType()
        {
            return SysConfig.PUBLISHER;
        }

        public override IRemoteEntity GetRemoteEntity()
        {
            return this.RemoteEntity;
        }
    }

    public class SubscriberEntity : Entity
    {
        private IRemoteSubscriber remoteEntity;

        public SubscriberEntity(String name, String url) : base(name, url) { }

        #region "properties"
        public IRemoteSubscriber RemoteEntity
        {
            get
            {
                return remoteEntity;
            }

            set
            {
                remoteEntity = value;
            }
        }
        #endregion

   
        public override List<Connection> GetConnectionsUrl()
        {
            List<Connection> connectionURLS = new List<Connection>();

            connectionURLS.AddRange(this.Site.GetBrokerUrls(this.Url));

            return connectionURLS;
        }

        
        public override String EntityType()
        {
            return SysConfig.SUBSCRIBER;
        }

        public override IRemoteEntity GetRemoteEntity()
        {
            return this.RemoteEntity;
        }
    }
}
