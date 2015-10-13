using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Shared_Library;

namespace PuppetMaster
{
    class Program
    {
        static void Main(string[] args)
        {

        }
    }

    class PuppetMaster
    {
        private static String CONFIG_FILE_PATH = @"../config/config.txt";
        private Dictionary<String, Entity> entities = new Dictionary<string, Entity>();

        /*
         *  Function that reads the first 
         */
        public void Start()
        {
            String line = null;

            Console.WriteLine("[INIT] Start reading configuration file...");
            try
            {
                StreamReader file = new StreamReader(CONFIG_FILE_PATH);

                while ((line = file.ReadLine()) != null)
                {
                    ProcessLine(line);
                }
            } catch (Exception e)
            {
                Console.WriteLine("[INIT] Failed to parse config file, exception: {0}", e.Message);
            }
        }

        #region "ConfigFileProcess"
        /*
         *  Function that parses one line from the config file
         */
        private void ProcessLine(string line)
        {
            String[] splitedLine = line.Split(' ');

            switch (splitedLine[0].ToLower())
            {
                case "site":
                    ProcessSite(splitedLine);
                    break;
                case "process":
                    ProcessProcess(splitedLine);
                    break;
                case "routingpolicy":
                    ProcessRouting(splitedLine);
                    break;
                case "ordering":
                    ProcessOrdering(splitedLine);
                    break;
                case "logginglevel":
                    ProcessLoggingLevel(splitedLine);
                    break;
                default:
                    break;
            }

        }


        /*
         *  Functions to process a config file line
         */

        private void ProcessLoggingLevel(string[] splitedLine)
        {
            throw new NotImplementedException();
        }

        private void ProcessOrdering(string[] splitedLine)
        {
            throw new NotImplementedException();
        }

        private void ProcessRouting(string[] splitedLine)
        {
            throw new NotImplementedException();
        }

        private void ProcessProcess(string[] splitedLine)
        {
            throw new NotImplementedException();
        }

        private void ProcessSite(string[] splitedLine)
        {
            throw new NotImplementedException();
        }
        #endregion
    }


    class SystemNetwork
    {
        #region "Attributes"
        private String logLevel = null;
        private String routingPolicy = null;
        private String ordering = null;
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
        #endregion
    }

}
