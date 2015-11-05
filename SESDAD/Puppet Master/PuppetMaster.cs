using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Shared_Library;
using Shared_Library_PM;
using System.Collections.Concurrent;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;
using System.Threading;

namespace PuppetMaster
{
    class Program
    {
        static void Main(string[] args)
        {
            PuppetMaster pm = new PuppetMaster();
            pm.Start();
        }
    }

    class PuppetMaster : MarshalByRefObject, IRemotePuppetMaster
    {
        private static String CONFIG_FILE_PATH = @"../../Config/config.txt";
        private static String SCRIPT_FILE_PATH = @"../../Config/script.txt";
        private static String EXIT_CMD = "exit";
        
        private SystemNetwork network = new SystemNetwork();
        public Logger log = new Logger();
        public Shell shell;

        private Dictionary<String, IRemotePuppetMasterSlave> pmSlaves = new Dictionary<string, IRemotePuppetMasterSlave>();

        private static Semaphore sem = new Semaphore(0, 1);
        private int maxNumberEntities;
        private static object lockObject = new object();
        private int entitiesProcessed = 0;

        public SystemNetwork Network
        {
            get
            {
                return network;
            }

            set
            {
                network = value;
            }
        }

        #region "Main Functions"
        public void Start()
        {
            Console.WriteLine("[INIT] Registering PuppetMaster Remote Object...");
            RegisterPM();
            Console.WriteLine("[INIT] Start reading configuration file...");
            ReadFile("config");
            log.newConnection();
            CreateNetwork();
            Console.WriteLine("[INIT] Successfully generated the network, processing script file...");
            ReadFile("script");
            Console.WriteLine("[INIT] Waiting input (exit to finish)");
            RunMode();
            Console.WriteLine("[INFO] Shutingdown the network...");
            ShutDownNetwork();
            Console.WriteLine("[INFO] All processes have been terminated, bye...");
        }

        private void RegisterPM()
        {
            TcpChannel channel = new TcpChannel(SysConfig.PM_PORT);
            ChannelServices.RegisterChannel(channel, false);
            RemotingServices.Marshal(this, SysConfig.PM_NAME, typeof(IRemotePuppetMaster));
        }

        private void WaitSlaves()
        {
            Console.ReadLine();
        }

        private void CreateNetwork()
        {

            if (!this.network.Distributed.Equals("localhost"))
            {
                Console.WriteLine("[INFO] Wainting Slaves to join the network...");
                WaitSlaves();
            }

            try
            {
                foreach (KeyValuePair<string, Entity> entry in network.Entities)
                {
                    if (Utils.GetIPDomain(entry.Value.Url).Equals(this.network.Distributed)) //check if the process is local to the pm machine
                    {
                        LaunchProcess(entry.Value);
                    }
                    else
                    {
                        LaunchRemoteProcess(entry.Value);
                    }
                }
            }
            catch (Exception)
            {
                //TODO 
            }

            this.maxNumberEntities = this.network.Entities.Count;
            SemaphoreWait(); //wait all processes to be up and running

            NotifyEntitiesToEstablishConnections();
            
            this.shell = new Shell(this.Network, this.log);
        }

        //notifies all entities that they can start establishing all conections
        private void NotifyEntitiesToEstablishConnections()
        {
            List<Thread> threads = new List<Thread>();

            foreach (KeyValuePair<string, Entity> entry in this.network.Entities)
            {
                Thread t = new Thread(entry.Value.GetRemoteEntity().EstablishConnections);
                t.Start();
                threads.Add(t);
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }
        }

        private void RunMode()
        {
            String cmd = "";

            while (!cmd.Equals(EXIT_CMD))
            {
                Console.Write("[CMD] > ");
                cmd = Console.ReadLine();

                shell.ProcessCommand(cmd);
            }

        }

        private void ShutDownNetwork()
        {

            foreach (KeyValuePair<string, Entity> entry in network.Entities)
            {
                try
                {
                    entry.Value.GetRemoteEntity().Disconnect();
                }
                catch (Exception)
                {
                }  

            }
        }
        #endregion

        #region "ConfigFileProcess"
        public void ReadFile(string fileName)
        {
            String line = null;
            int lineNr = 0;
            StreamReader file = null;
            String path;

            try
            {
                if (fileName.Equals("config"))
                    path = CONFIG_FILE_PATH;
                else
                    path = SCRIPT_FILE_PATH;

                file = new StreamReader(path);

                while ((line = file.ReadLine()) != null)
                {
                    if (fileName.Equals("config"))
                        ProcessConfigLine(line, lineNr++);
                    else
                        shell.ProcessCommand(line, lineNr++);
                }
                Console.WriteLine("[INIT] Successfully parsed configuration file, deploying network...");
            }
            catch (Exception e)
            {
                Console.WriteLine("[INIT] Failed to parse {0} file, exception: {1}", fileName, e.Message);
            }
            finally
            {
                if (file != null) file.Close();
            }
        }



        /*
         *  Function that parses one line from the config file
         */
        private void ProcessConfigLine(string line, int lineNr)
        {
            String[] splitedLine = line.Split(' ');

            switch (splitedLine[0].ToLower())
            {
                case "site":
                    ProcessSite(splitedLine, lineNr);
                    break;
                case "process":
                    ProcessProcess(splitedLine, lineNr);
                    break;
                case "routingpolicy":
                    ProcessRouting(splitedLine, lineNr);
                    break;
                case "ordering":
                    ProcessOrdering(splitedLine, lineNr);
                    break;
                case "logginglevel":
                    ProcessLoggingLevel(splitedLine, lineNr);
                    break;
                case "distributed":
                    ProcessDistributed(splitedLine, lineNr);
                    break;
                default:
                    break;
            }

        }




        /*
         *  Functions to process a config file line
         */
        private void ProcessDistributed(string[] splitedLine, int lineNr)
        {
            if (splitedLine.Length != 2)
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [Distributed]");
            }

            this.network.Distributed = splitedLine[1];
        }

        private void ProcessOrdering(string[] splitedLine, int lineNr)
        {
            if (splitedLine.Length != 2 || (!SysConfig.NO_ORDER.Equals(splitedLine[1].ToLower()) && !SysConfig.FIFO.Equals(splitedLine[1].ToLower()) && !SysConfig.TOTAL.Equals(splitedLine[1].ToLower())))
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [Ordering]");
            }

            this.network.Ordering = splitedLine[1].ToLower();
        }

        private void ProcessLoggingLevel(string[] splitedLine, int lineNr)
        {
            if (splitedLine.Length != 2 || (!SysConfig.FULL.Equals(splitedLine[1].ToLower()) && !SysConfig.LIGHT.Equals(splitedLine[1].ToLower())))
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [LoggingLevel]");
            }

            this.network.LogLevel = splitedLine[1].ToLower();
        }

        private void ProcessRouting(string[] splitedLine, int lineNr)
        {
            if (splitedLine.Length != 2 || (!SysConfig.FLOODING.Equals(splitedLine[1].ToLower()) && !SysConfig.FILTER.Equals(splitedLine[1].ToLower())))
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [RoutingPolicy]");
            }

            this.network.RoutingPolicy = splitedLine[1].ToLower();
        }

        private void ProcessProcess(string[] splitedLine, int lineNr)
        {
            String targetEntityName, entityType, siteName, url;
            Site parentSite;

            if (splitedLine.Length != 8)
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [Process]");
            }

            targetEntityName = splitedLine[1].ToLower();
            entityType = splitedLine[3].ToLower();
            siteName = splitedLine[5].ToLower();
            url = splitedLine[7].ToLower();

            if (!network.SiteMap.TryGetValue(siteName, out parentSite))
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [Process] site: " + siteName + " does not exist");
            }

            switch (entityType)
            {
                case SysConfig.BROKER:
                    BrokerEntity bEntity = new BrokerEntity(targetEntityName, url);
                    bEntity.Site = parentSite;
                    parentSite.BrokerEntities.Add(targetEntityName, bEntity);
                    network.AddEntity(bEntity);
                    break;
                case SysConfig.PUBLISHER:
                    PublisherEntity pEntity = new PublisherEntity(targetEntityName, url);
                    pEntity.Site = parentSite;
                    parentSite.PublisherEntities.Add(targetEntityName, pEntity);
                    network.AddEntity(pEntity);
                    break;
                case SysConfig.SUBSCRIBER:
                    SubscriberEntity sEntity = new SubscriberEntity(targetEntityName, url);
                    sEntity.Site = parentSite;
                    parentSite.SubscriberEntities.Add(targetEntityName, sEntity);
                    network.AddEntity(sEntity);
                    break;
                default:
                    break;
            }

        }

        private void ProcessSite(string[] splitedLine, int lineNr)
        {
            String targetSiteName, parentSiteName;
            Site targetSite, parentSite;

            if (splitedLine.Length != 4)
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [Site]");
            }

            targetSiteName = splitedLine[1].ToLower();
            parentSiteName = splitedLine[3].ToLower();

            if (!network.SiteMap.TryGetValue(targetSiteName, out targetSite))
            {
                targetSite = new Site(targetSiteName);
            }

            if (!"none".Equals(parentSiteName))
            {
                if (!network.SiteMap.TryGetValue(parentSiteName, out parentSite))
                {
                    parentSite = new Site(parentSiteName);
                    network.AddSite(parentSite);
                }

                parentSite.Children.Add(targetSite);
            }
            else
            {
                parentSite = null;
            }


            targetSite.Parent = parentSite;
            network.AddSite(targetSite);

        }
        #endregion

        #region "NetworkCreation"


        private void LaunchRemoteProcess(Entity ent)
        {
            IRemotePuppetMasterSlave slave;
            string ipDomain = Utils.GetIPDomain(ent.Url);
            if (!this.pmSlaves.TryGetValue(ipDomain, out slave))
            {
                Console.WriteLine("[ERROR] Slave for ipdomain: " + ipDomain + "not found and process not launched");
            }

            slave.StartNewProcess(ent.Name, ent.EntityType(), ent.Url);
        }

        private void LaunchProcess(Entity ent)
        {
            String args = String.Format("{0} {1} tcp://localhost:{2}/{3}", ent.Name, ent.Url, SysConfig.PM_PORT, SysConfig.PM_NAME);
            ProcessManager.LaunchProcess(ent.EntityType(), args);
        }

        #endregion

        #region "Interface Methods"
        public void RegisterSlave(String url)
        {

            try
            {
                IRemotePuppetMasterSlave newSlave = (IRemotePuppetMasterSlave)Activator.GetObject(typeof(IRemotePuppetMasterSlave), url);
                String ipDomain = Utils.GetIPDomain(url);

                lock (this)
                {
                    this.pmSlaves.Add(ipDomain, newSlave);
                }

                Console.WriteLine("[INFO] Added PM Slave for domain:'" + ipDomain + "'");
            }
            catch (Exception)
            {
                Console.WriteLine("[ERROR] Failed to add PM Slave");
            }

        }

        public void RegisterBroker(string url, string name)
        {
            BrokerEntity bEntity = (BrokerEntity)this.network.GetEntity(name);
            IRemoteBroker newBroker = (IRemoteBroker)Activator.GetObject(typeof(IRemoteBroker), url);
            bEntity.RemoteEntity = newBroker;
            SysConfig config = this.network.SystemConfig.cloneConfig();
            config.Connections = bEntity.GetConnectionsUrl();
            newBroker.RegisterInitializationInfo(config);
            PostEntityProcessed();

            Console.WriteLine(String.Format("[INFO] Broker: {0} connected on url: {1}", name, url));
        }

        public void RegisterPublisher(string url, string name)
        {
            PublisherEntity pEntity = (PublisherEntity)this.network.GetEntity(name);
            IRemotePublisher newPublisher = (IRemotePublisher)Activator.GetObject(typeof(IRemotePublisher), url);
            pEntity.RemoteEntity = newPublisher;
            SysConfig config = this.network.SystemConfig.cloneConfig();
            config.Connections = pEntity.GetConnectionsUrl();
            newPublisher.RegisterInitializationInfo(config);
            PostEntityProcessed();

            Console.WriteLine(String.Format("[INFO] Publisher: {0} connected on url: {1}", name, url));
        }

        public void RegisterSubscriber(string url, string name)
        {
            SubscriberEntity sEntity = (SubscriberEntity) this.network.GetEntity(name);
            IRemoteSubscriber newSubscriber = (IRemoteSubscriber)Activator.GetObject(typeof(IRemoteSubscriber), url);
            sEntity.RemoteEntity = newSubscriber;
            SysConfig config = this.network.SystemConfig.cloneConfig();
            config.Connections = sEntity.GetConnectionsUrl();
            newSubscriber.RegisterInitializationInfo(config);
            PostEntityProcessed();

            Console.WriteLine(String.Format("[INFO] Subscriber: {0} connected on url: {1}", name, url));
        }


        public void Notify(string msg)
        {
            Console.WriteLine("[Notify] " + msg);
        }

        public void LogEventPublication(string publisher, string topicname, int eventNumber)
        {
            log.LogEventPublication(publisher, topicname, eventNumber);
        }

        public void LogEventForwarding(string broker, string publisher, string topicname, int eventNumber)
        {
            log.LogEventForwarding(broker, publisher, topicname, eventNumber);
        }

        public void LogEventDelivery(string subscriber, string publisher, string topicname, int eventNumber)
        {
            log.LogEventDelivery(subscriber, publisher, topicname, eventNumber);
        }

        public void PostEntityProcessed()
        {
            lock (this)
            {
                if (++this.entitiesProcessed == this.maxNumberEntities)
                    SemaphoreRelease();
            }
        }
        #endregion

        #region "Semaphores"
        public void SemaphoreWait()
        {
            PuppetMaster.sem.WaitOne();
        }

        public void SemaphoreRelease()
        {
            PuppetMaster.sem.Release();
        }

        #endregion

    }
}
