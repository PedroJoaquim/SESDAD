using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shared_Library
{
    public interface RemoteEntity
    {
        void sendMenssage(String message);
    }

    public class Site
    {
        private Site parent = null;
        private List<Site> childrens = new List<Site>(); 
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
        #endregion
    }

    public class Entity
    {
        private Site site = null;
        private String name = null;
        private String url = null;
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
        #endregion

    }
}
