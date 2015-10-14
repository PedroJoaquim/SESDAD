using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;

namespace PuppetMaster
{


    public class Site
    {

        private string name = null;
        private Site parent = null;
        private List<Site> children = new List<Site>();
        private Dictionary<String, Entity> entities = new Dictionary<string, Entity>(); //brokers + subscribers + publishers associated with the site

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
        #endregion

        public Site(String name)
        {
            this.name = name;
        }

        /*
         * Function that returns a list with the URL of all entities of the given type
         */

        public List<Tuple<String, String>> GetEntitiesUrls(string type, string ignoreCase = "")
        {
            List<Tuple<String, String>> connectionURLS = new List<Tuple<String, String>>();

            foreach (KeyValuePair<string, Entity> entry in this.Entities)
            {
                if (entry.Value.Type.Equals(type) && !entry.Value.Url.Equals(ignoreCase))
                {
                    connectionURLS.Add(Tuple.Create(entry.Value.Url, entry.Value.Type));
                }
            }

            return connectionURLS;
        }


        /*
         *  Function that get all broker's urls that have a connection to this site
         */
        public List<Tuple<String, String>> GetOutsideBrokersUrls(string ignoreCase = "")
        {
            List<Tuple<String, String>> connectionURLS = new List<Tuple<String, String>>();

            if (this.Parent != null)
            {
                connectionURLS.AddRange(this.Parent.GetEntitiesUrls(Entity.BROKER, ignoreCase));
            }

            foreach (Site currentSite in this.Children)
            {
                connectionURLS.AddRange(currentSite.GetEntitiesUrls(Entity.BROKER, ignoreCase));
            }

            return connectionURLS;
        }


        /*
         * Function that returns a list with all the entities of the given type
         */
        public List<Entity> GetAllEntities(String type)
        {
            List<Entity> entitiesList = new List<Entity>();

            foreach (KeyValuePair<string, Entity> entry in this.Entities)
            {
                if (entry.Value.Type.Equals(type))
                {
                    entitiesList.Add(entry.Value);
                }
            }

            return entitiesList;
        }
    }


    public class Entity
    {

        public static String BROKER = "broker";
        public static String PUBLISHER = "publisher";
        public static String SUBSCRIBER = "subscriber";


        private Site site = null;
        private String name = null;
        private String url = null;
        private String type = null;
        private RemoteEntity entityProxy = null;

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

        public RemoteEntity EntityProxy
        {
            get
            {
                return entityProxy;
            }

            set
            {
                entityProxy = value;
            }
        }

        public string Type
        {
            get
            {
                return type;
            }

            set
            {
                type = value;
            }
        }
        #endregion

        public Entity(String name, String url, String type)
        {
            this.name = name;
            this.url = url;
            this.type = type;
        }

        public List<Tuple<String, String>> GetConnectionsUrl()
        {
            List<Tuple<String, String>> connectionURLS = new List<Tuple<String, String>>();

            if (BROKER.Equals(type))
            {
                //brokers have connections with other brokers in other sites and all publishers and subscribers in this site
                connectionURLS.AddRange(this.Site.GetOutsideBrokersUrls(this.Url));
                connectionURLS.AddRange(this.Site.GetEntitiesUrls(PUBLISHER, this.Url));
                connectionURLS.AddRange(this.Site.GetEntitiesUrls(SUBSCRIBER, this.Url));
                connectionURLS.AddRange(this.Site.GetEntitiesUrls(BROKER, this.Url));
            }
            else if (PUBLISHER.Equals(type) || SUBSCRIBER.Equals(type))
            {
                //publishers and subscribers have connections only with brokers from their site
                connectionURLS.AddRange(this.Site.GetEntitiesUrls(BROKER, this.Url));
            }

            return connectionURLS;
        }
    }
}
