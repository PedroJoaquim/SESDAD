using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;
using Shared_Library_PM;
using System.IO;

namespace Puppet_Master_Slave
{
    class PuppetMasterSlave : MarshalByRefObject, IRemotePuppetMasterSlave
    {
        private static string CONFIG_FILE_PATH = @"../../Config/slave_config.txt";
        private static int PM_URL_INDEX = 0;
        private static int MY_URL_INDEX = 1;
        private String pmURL;
 
        static void Main(string[] args)
        {
            PuppetMasterSlave pmSlave = new PuppetMasterSlave();
            pmSlave.Start();
            Console.ReadLine();
        }

        public void Start()
        {
            string[] urls = GetConfigFileInfo();
            String myUrl = urls[MY_URL_INDEX];
            String pmUrl = urls[PM_URL_INDEX];
            int myPort;

            if (!Int32.TryParse(Utils.GetIPPort(myUrl), out myPort)) myPort = SysConfig.PM_SLAVE_PORT;

            TcpChannel chan = new TcpChannel(SysConfig.PM_SLAVE_PORT);
            ChannelServices.RegisterChannel(chan, false);
            RemotingServices.Marshal(this, SysConfig.PM_SLAVE_NAME, typeof(PuppetMasterSlave));

            IRemotePuppetMaster pm = (IRemotePuppetMaster) Activator.GetObject(typeof(IRemotePuppetMaster), pmUrl);
            pm.RegisterSlave(myUrl);
            this.pmURL = pmUrl;

            Console.WriteLine("[INIT] Successfully connected to PuppetMaster");
        }


        private string[] GetConfigFileInfo()
        {
            String line = null;
            int lineNr = 0;
            string[] result = new String[2];

            try
            {
                StreamReader file = new StreamReader(CONFIG_FILE_PATH);

                while ((line = file.ReadLine()) != null && lineNr < 2)
                {
                    result[lineNr] = line.Trim();
                    lineNr++;
                }
            } catch (Exception e)
            {
                Console.WriteLine("[INIT] Failed to parse config file, exception: {0}", e.Message);
            }

            return result;
        }

        public void StartNewProcess(string objName, string objType, string objUrl)
        {
            String args = String.Format("{0} {1} {2}", objName, objUrl, this.pmURL);
            ProcessManager.LaunchProcess(objType, args);
        }
    }
}
