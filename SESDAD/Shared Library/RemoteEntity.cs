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
        private int numThreads;

        private IRemotePuppetMaster puppetMaster;
        private SysConfig sysConfig;
        private RemoteNetwork remoteNetwork;
        private EventQueue events;
        private TimeoutMonitor tMonitor;

        private static bool freeze = false;
       
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

        public RemoteNetwork RemoteNetwork
        {
            get
            {
                return remoteNetwork;
            }

            set
            {
                remoteNetwork = value;
            }
        }

        public TimeoutMonitor TMonitor
        {
            get
            {
                return tMonitor;
            }

            set
            {
                tMonitor = value;
            }
        }
        #endregion

        public RemoteEntity(String name, String url, String pmUrl, int queueSize, int numThreads)
        {
            this.Name = name;
            this.Url = url;
            this.PmURL = pmUrl;
            this.events = new EventQueue(queueSize);
            this.RemoteNetwork = new RemoteNetwork();
            this.tMonitor = new TimeoutMonitor(this);
            this.numThreads = numThreads;
        }

        public void Start()
        {
            Thread t;

            Console.WriteLine(String.Format("================== {0} ==================", Name));
            Register();

            //launch workers
            for (int i = 0; i < this.numThreads; i++)
            {
                t = new Thread(ProcessQueue);
                t.Start();
            }
            
            Console.ReadLine();
        }


        #region "Interface methods"

        public abstract void Register();
        public abstract void Status();

        public void RegisterInitializationInfo(SysConfig sysConfig, string siteName)
        {
            this.SysConfig = sysConfig;
            this.RemoteNetwork.SiteName = siteName;
        }

        public void EstablishConnections()
        {
            this.RemoteNetwork.Initialize(this.SysConfig);
            PuppetMaster.PostEntityProcessed();
        }


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
            lock (this)
            {
                freeze = true;
            }

        }

        public void Unfreeze()
        {
            lock (this)
            {
                freeze = false;
                Monitor.PulseAll(this);
            }
        }

        protected void CheckFreeze()
        {
            lock(this)
            {
                while(freeze)
                {
                    Monitor.Wait(this);
                }
            }
        }
        #endregion


        //functions that take care of timedout actions with no ack
        public abstract void ActionTimedout(DifundPublishEventProperties properties);
        public abstract void ActionTimedout(DifundSubscribeEventProperties properties);


        private void ProcessQueue()
        {
            Command command;
            Random rnd = new Random();

            while (true)
            {
                CheckFreeze();
                command = events.Consume();
                command.Execute(this); 
            }

        }

        public void Disconnect()
        {
            Environment.Exit(0);
        }


    }
}
