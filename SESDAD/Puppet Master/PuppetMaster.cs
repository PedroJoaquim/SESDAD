﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Shared_Library;
using Shared_Library_PM;

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

    class PuppetMaster : IRemotePuppetMaster
    {
        private static String CONFIG_FILE_PATH = @"../../Config/config.txt";
        private static String EXIT_CMD = "exit";

        private SystemNetwork network = new SystemNetwork();

        #region "Main Functions"
        public void Start()
        {
            Console.WriteLine("[INFO] Wainting Slaves to join the network...");
            WaitSlaves();
            Console.WriteLine("[INFO] Start reading configuration file...");
            ReadConfigFile();
            Console.WriteLine("[INFO] Successfully parsed configuration file, deploying network...");
            CreateNetwork();
            Console.WriteLine("[INFO] Successfully generated the network, waiting input...");
            RunMode();
            Console.WriteLine("[INFO] Shutingdown the network...");
            ShutDownNetwork();
            Console.WriteLine("[INFO] All processes have been terminated, bye...");
        }

        private void WaitSlaves()
        {
            return; // TODO 
        }

        private void CreateNetwork()
        {
            try
            {
                foreach (KeyValuePair<string, Entity> entry in network.Entities)
                {
                    if(entry.Value.IsLocal())
                    {
                        LaunchProcess(entry.Value);
                    }
                    else
                    {
                        LaunchRemoteProcess(entry.Value);
                    }
                }
            }
            catch(Exception)
            {
                //TODO 
            }

        }


        private void RunMode()
        {
            String cmd = "";

            while(!cmd.Equals(EXIT_CMD))
            {
                cmd = Console.ReadLine();
                processCommand(cmd);
            }
        }

        private void ShutDownNetwork()
        {
            throw new NotImplementedException();
        }
        #endregion

        #region "ConfigFileProcess"
        public void ReadConfigFile()
        {
            String line = null;
            int lineNr = 0;

            try
            {
                StreamReader file = new StreamReader(CONFIG_FILE_PATH);

                while ((line = file.ReadLine()) != null)
                {
                    ProcessLine(line, lineNr++);
                }
            } catch (Exception e)
            {
                Console.WriteLine("[INIT] Failed to parse config file, exception: {0}", e.Message);
            }
        }
        
        
        /*
         *  Function that parses one line from the config file
         */
        private void ProcessLine(string line, int lineNr)
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
                default:
                    break;
            }

        }


        /*
         *  Functions to process a config file line
         */

        private void ProcessOrdering(string[] splitedLine, int lineNr)
        {
            if (splitedLine.Length != 2 || (!"no".Equals(splitedLine[1].ToLower()) && !"fifo".Equals(splitedLine[1].ToLower()) && !"total".Equals(splitedLine[1].ToLower())))
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [Ordering]");
            }

            this.network.Ordering = splitedLine[1].ToLower();
        }

        private void ProcessLoggingLevel(string[] splitedLine, int lineNr)
        {
            if(splitedLine.Length != 2 || (!"full".Equals(splitedLine[1].ToLower()) && !"light".Equals(splitedLine[1].ToLower())))
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [LoggingLevel]");
            }

            this.network.LogLevel = splitedLine[1].ToLower();
        }

        private void ProcessRouting(string[] splitedLine, int lineNr)
        {
            if (splitedLine.Length != 2 || (!"flooding".Equals(splitedLine[1].ToLower()) && !"filter".Equals(splitedLine[1].ToLower())))
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

            if(!network.SiteMap.TryGetValue(siteName, out parentSite))
            {
                throw new ConfigFileParseException("[Line " + lineNr + "]" + "Error in entry [Process] site: " + siteName + " does not exist");
            }

            switch (entityType)
            {
                case Entity.BROKER:
                    BrokerEntity bEntity = new BrokerEntity(targetEntityName, url);
                    bEntity.Site = parentSite;
                    parentSite.BrokerEntities.Add(targetEntityName, bEntity);
                    network.AddEntity(bEntity);
                    break;
                case Entity.PUBLISHER:
                    PublisherEntity pEntity = new PublisherEntity(targetEntityName, url);
                    pEntity.Site = parentSite;
                    parentSite.PublisherEntities.Add(targetEntityName, pEntity);
                    network.AddEntity(pEntity);
                    break;
                case Entity.SUBSCRIBER:
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

            if(!network.SiteMap.TryGetValue(targetSiteName, out targetSite))
            {
                targetSite = new Site(targetSiteName);
            }
            
            if(!"none".Equals(parentSiteName))
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


        private void LaunchRemoteProcess(Entity value)
        {
            throw new NotImplementedException();
        }

        private bool IsRemoteEntity(Entity value)
        {
            return !value.Url.ToLower().Contains("localhost") && !value.Url.ToLower().Contains("127.0.0.1");
        }

        private void LaunchProcess(Entity ent)
        {
            List<Tuple<String, String>> connections = ent.GetConnectionsUrl();

            ProcessManager.LaunchProcess(ent.EntityType(), "TODO");

            return;
        }
        #endregion

        #region "RunMode"
        private void processCommand(string cmd)
        {
            throw new NotImplementedException();
        }
        #endregion
    }


    

}
