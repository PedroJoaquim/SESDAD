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
            this.systemConfig.LogLevel = "light";
            this.systemConfig.RoutingPolicy = "flooding";
            this.systemConfig.Ordering = "fifo";
        }


        #region "Properties"
        public string LogLevel
        {
            get
            {
                return this.systemConfig.LogLevel;
            }

            set
            {
                this.systemConfig.LogLevel = value;
            }
        }

        public string RoutingPolicy
        {
            get
            {
                return this.systemConfig.RoutingPolicy;
            }

            set
            {
                this.systemConfig.RoutingPolicy = value;
            }
        }

        public string Ordering
        {
            get
            {
                return this.systemConfig.Ordering;
            }

            set
            {
                this.systemConfig.Ordering = value;
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
        public List<Tuple<String, String>> GetOutsideBrokersUrls(string ignoreCase = "")
        {
            List<Tuple<String, String>> connectionURLS = new List<Tuple<String, String>>();

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

        public List<Tuple<string, string>> GetPublisherUrls(string ignoreCase = "")
        {
            List<Tuple<string, string>> result = new List<Tuple<string, string>>();

            foreach (KeyValuePair<string, PublisherEntity> entry in this.PublisherEntities)
            {
                if (!entry.Value.Url.Equals(ignoreCase))
                {
                    result.Add(Tuple.Create(entry.Value.Url, Entity.PUBLISHER));
                }
            }

            return result;
        }

        public List<Tuple<string, string>> GetSubscriberUrls(string ignoreCase = "")
        {
            List<Tuple<string, string>> result = new List<Tuple<string, string>>();

            foreach (KeyValuePair<string, SubscriberEntity> entry in this.SubscriberEntities)
            {
                if (!entry.Value.Url.Equals(ignoreCase))
                {
                    result.Add(Tuple.Create(entry.Value.Url, Entity.SUBSCRIBER));
                }
            }

            return result;
        }

        public List<Tuple<string, string>> GetBrokerUrls(string ignoreCase = "")
        {
            List<Tuple<string, string>> result = new List<Tuple<string, string>>();

            foreach (KeyValuePair<string, BrokerEntity> entry in this.BrokerEntities)
            {
                if (!entry.Value.Url.Equals(ignoreCase))
                {
                    result.Add(Tuple.Create(entry.Value.Url, Entity.BROKER));
                }
            }

            return result;
        }



    }


    public abstract class Entity
    {

        private Site site = null;
        private String name = null;
        private String url = null;

        public const String BROKER = "broker";
        public const String PUBLISHER = "publisher";
        public const String SUBSCRIBER = "subscriber";

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

        public bool IsLocal()
        {
            return !this.Url.ToLower().Contains("localhost") && !this.Url.ToLower().Contains("127.0.0.1");
        }

        public abstract List<Tuple<String, String>> GetConnectionsUrl();
        public abstract String EntityType();
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

        override
        public List<Tuple<String, String>> GetConnectionsUrl()
        {
            List<Tuple<String, String>> connectionURLS = new List<Tuple<string, string>>();

            //brokers have connections with other brokers in other sites and all publishers and subscribers in this site
            connectionURLS.AddRange(this.Site.GetOutsideBrokersUrls(this.Url));
            connectionURLS.AddRange(this.Site.GetPublisherUrls(this.Url));
            connectionURLS.AddRange(this.Site.GetSubscriberUrls(this.Url));
            connectionURLS.AddRange(this.Site.GetBrokerUrls(this.Url));

            return connectionURLS;
        }

        override
        public String EntityType()
        {
            return Entity.BROKER;
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

        override
        public List<Tuple<String, String>> GetConnectionsUrl()
        {
            List<Tuple<String, String>> connectionURLS = new List<Tuple<string, string>>();

            connectionURLS.AddRange(this.Site.GetBrokerUrls(this.Url));

            return connectionURLS;
        }

        override
        public String EntityType()
        {
            return Entity.PUBLISHER;
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

        override
        public List<Tuple<String, String>> GetConnectionsUrl()
        {
            List<Tuple<String, String>> connectionURLS = new List<Tuple<string, string>>();

            connectionURLS.AddRange(this.Site.GetBrokerUrls(this.Url));

            return connectionURLS;
        }

        override
        public String EntityType()
        {
            return Entity.SUBSCRIBER;
        }
    }
}
