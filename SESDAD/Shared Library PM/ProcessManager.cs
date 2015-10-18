using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Shared_Library_PM
{
    public class ProcessManager
    {
        private const String BROKER_PROCESS = @"..\broker\Broker.exe";
        private const String PUBLISHER_PROCESS = @"..\publisher\Publisher.exe";
        private const String SUBSCRIBER_PROCESS = @"..\subscriber\Publisher.exe";

        public static void LaunchProcess(String processType, String args)
        {
            Process process = new Process();
            process.StartInfo.FileName = BROKER_PROCESS;
            process.StartInfo.Arguments = args;
            process.Start();

        }

        private String getProcessPath(String type)
        {
            return "TODO";
        }
    }
}
