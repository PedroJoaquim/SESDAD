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
        void RegisterSlave(String url);
        void RegisterBroker(String url, String name);
        void RegisterPublisher(String url, String name);
        void RegisterSubscriber(String url, String name);
    }

    public interface IRemotePuppetMasterSlave
    {
        void StartNewProcess(String objName, String objType, String objUrl);
    }

    public class SysConfig
    {
        public const int PM_PORT = 56000;
        public const int PM_SLAVE_PORT = 55000;
        public const String PM_NAME = "PuppetMaster";
        public const String PM_SLAVE_NAME = "PuppetMasterSlave";
        public const String BROKER = "broker";
        public const String PUBLISHER = "publisher";
        public const String SUBSCRIBER = "subscriber";

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
    }
}
