using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shared_Library
{
    public interface IRemoteBroker
    {
        //TODO
    }

    public interface IRemotePublisher
    {
        //TODO
    }

    public interface IRemoteSubscriber
    {
        //TODO
    }

    public interface IRemotePuppetMaster
    {
        //TODO
    }

    public class SysConfig
    {
        #region "Attributes"
        private String logLevel = null;
        private String routingPolicy = null;
        private String ordering = null;
        private String connections = null;
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

        public string Connections
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

    }

}
