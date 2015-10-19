using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shared_Library
{
    public interface IRemoteEntity
    {
        void RegisterInitializationInfo(SysConfig sysConfig);
        void EstablishConnections();
        string GetEntityName();

        void Status();
        void Crash();
        void Freeze();
        void Unfreeze();
    }

    public interface IRemoteBroker : IRemoteEntity
    {
        void DifundPublishEvent(String topic);
        void DifundSubscribeEvent(String topic);
        void DifundUnSubscribeEvent(String topic);
    }

    public interface IRemotePublisher : IRemoteEntity
    {
        void Publish(String topic, int nrEvents, int ms);

    }

    public interface IRemoteSubscriber : IRemoteEntity
    {
        void Subscribe(String topic);
        void Unsubscribe(String topic);
    }

    public interface IRemotePuppetMaster
    {
        void RegisterSlave(String url);
        void RegisterBroker(String url, String name);
        void RegisterPublisher(String url, String name);
        void RegisterSubscriber(String url, String name);
        void Wait(int x_ms);
        void Notify(String msg);
    }

    public interface IRemotePuppetMasterSlave
    {
        void StartNewProcess(String objName, String objType, String objUrl);
    }

    public abstract class RemoteEntity : MarshalByRefObject, IRemoteEntity
    {
        #region "Attributes"
        private String name;
        private String url;
        private String pmURL;

        private SysConfig sysConfig;
        private Dictionary<String, IRemoteBroker> brokers = new Dictionary<string, IRemoteBroker>();
        private Dictionary<String, IRemotePublisher> publishers = new Dictionary<string, IRemotePublisher>();
        private Dictionary<String, IRemoteSubscriber> subscribers = new Dictionary<string, IRemoteSubscriber>();

        private Queue<Command> commands = new Queue<Command>();
        private static Semaphore freezeSemaphore = new Semaphore(1, 1);
        private static Semaphore queueSemaphore = new Semaphore(0, 1);
        #endregion

        #region Properties
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

        public string PmURL
        {
            get
            {
                return pmURL;
            }

            set
            {
                pmURL = value;
            }
        }

        public SysConfig SysConfig
        {
            get
            {
                return sysConfig;
            }

            set
            {
                sysConfig = value;
            }
        }

        public Dictionary<string, IRemoteBroker> Brokers
        {
            get
            {
                return brokers;
            }

            set
            {
                brokers = value;
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
        #endregion

        public RemoteEntity(String name, String url, String pmUrl)
        {
            this.Name = name;
            this.Url = url;
            this.PmURL = pmUrl;
        }

        public void Start()
        {
           Register();
           Thread t = new Thread(ProcessQueue);
           t.Start();
        }


        #region "Initialization"
        public void RegisterInitializationInfo(SysConfig sysConfig)
        {
            this.SysConfig = sysConfig;
        }

        public void EstablishConnections()
        {
            foreach (Tuple<String, String> conn in this.SysConfig.Connections)
            {
                switch (conn.Item2)
                {
                    case SysConfig.BROKER:
                        IRemoteBroker newBroker = (IRemoteBroker)Activator.GetObject(typeof(IRemoteBroker), conn.Item1);
                        this.Brokers.Add(newBroker.GetEntityName(), newBroker); 
                        break;
                    case SysConfig.SUBSCRIBER:
                        IRemoteSubscriber newSubscriber = (IRemoteSubscriber)Activator.GetObject(typeof(IRemoteSubscriber), conn.Item1);
                        this.Subscribers.Add(newSubscriber.GetEntityName(), newSubscriber);
                        break;
                    case SysConfig.PUBLISHER:
                        IRemotePublisher newPublisher = (IRemotePublisher)Activator.GetObject(typeof(IRemotePublisher), conn.Item1);
                        this.Publishers.Add(newPublisher.GetEntityName(), newPublisher);
                        break;
                    default:
                        break;
                }

                Console.WriteLine(String.Format("[INFO] {0} added on: {1}", conn.Item2, conn.Item1));
            }
          
        }
        #endregion

        public abstract void Register();
        public abstract void Status();

        public string GetEntityName()
        {
            return this.Name;
        }

        public void Crash()
        {
            throw new NotImplementedException();
        }

        public void Freeze()
        {
            freezeSemaphore.WaitOne();
        }

        public void Unfreeze()
        {
            freezeSemaphore.Release();
        }

        private void ProcessQueue()
        {
            freezeSemaphore.WaitOne();

        }
    }

    [Serializable()]
    public class SysConfig : ISerializable
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
        private List<Tuple<String, String>> connections = null;
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

        public List<Tuple<string, string>> Connections
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

        #region "Serialization"
        public SysConfig(SerializationInfo info, StreamingContext ctxt)
        {
            //Get the values from info and assign them to the appropriate properties
            logLevel = (String) info.GetValue("logLevel", typeof(String));
            routingPolicy = (String)info.GetValue("routingPolicy", typeof(String));
            ordering = (String)info.GetValue("ordering", typeof(String));
            connections = DeserializeConnections((String) info.GetValue("connections", typeof(String)));
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

            if (this.Connections != null)
            {
                foreach (Tuple<String, String> conn in this.Connections)
                {
                    result += conn.Item1 + "#" + conn.Item2 + "#";
                }
            }

            return result.Remove(result.Length - 1);
         } 

        private List<Tuple<String, String>> DeserializeConnections(String connStr)
        {
            List<Tuple<String, String>> result = new List<Tuple<string, string>>();

            string[] splitedConns = connStr.Split('#');

            for (int i = 0; i < splitedConns.Length - 1; i = i +2)
            {
                result.Add(new Tuple<string, string>(splitedConns[i], splitedConns[i + 1]));
            }


            return result;
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

    public abstract class Command
    {
        public abstract void Execute(IRemoteEntity entity);
    }
}
