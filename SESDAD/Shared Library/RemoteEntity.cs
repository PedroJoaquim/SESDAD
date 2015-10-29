using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shared_Library
{

    public abstract class RemoteEntity : MarshalByRefObject, IRemoteEntity
    {
        #region "Attributes"
        private String name;
        private String url;
        private String pmURL;

        private IRemotePuppetMaster puppetMaster;
        private SysConfig sysConfig;
        private Dictionary<String, IRemoteBroker> brokers = new Dictionary<string, IRemoteBroker>();
        private Dictionary<String, IRemotePublisher> publishers = new Dictionary<string, IRemotePublisher>();
        private Dictionary<String, IRemoteSubscriber> subscribers = new Dictionary<string, IRemoteSubscriber>();

        private EventQueue events = new EventQueue(50);
        private static Semaphore freezeSemaphore = new Semaphore(1, 1); //semaphore for freeze command
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

        public IRemotePuppetMaster PuppetMaster
        {
            get
            {
                return puppetMaster;
            }

            set
            {
                puppetMaster = value;
            }
        }

        public EventQueue Events
        {
            get
            {
                return events;
            }

            set
            {
                events = value;
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
            Console.WriteLine(String.Format("================== {0} ==================", Name));
            Register();
            Thread t = new Thread(ProcessQueue);
            t.Start();
            Console.ReadLine();
        }


        #region "Interface methods"

        //not yet implemented
        public abstract void Register();
        public abstract void Status();


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

            PuppetMaster.PostEntityProcessed();

        }
        #endregion

        public string GetEntityName()
        {
            return this.Name;
        }

        public void Crash()
        {
            Disconnect();
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
            Command command;

            while (true)
            {
                command = events.Consume();
                freezeSemaphore.WaitOne();       //see if the process is freeze
                command.Execute(this);
                freezeSemaphore.Release();
            }

        }

        public void Disconnect()
        {
            Environment.Exit(0);
        }
    }
}
