using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using Shared_Library;

namespace Shared_Library_PM
{
    public class ProcessManager
    {
        private const String BROKER_PROCESS = @"..\broker\Broker.exe";
        private const String PUBLISHER_PROCESS = @"..\publisher\Publisher.exe";
        private const String SUBSCRIBER_PROCESS = @"..\subscriber\Subscriber.exe";

        public static void LaunchProcess(String processType, String args)
        {
            Process process = new Process();
            process.StartInfo.FileName = GetProcessPath(processType);
            process.StartInfo.Arguments = args;
            process.Start();

        }

        private static String GetProcessPath(String type)
        {
            switch (type)
            {
                case SysConfig.BROKER:
                    return BROKER_PROCESS;
                case SysConfig.PUBLISHER:
                    return PUBLISHER_PROCESS;
                case SysConfig.SUBSCRIBER:
                    return SUBSCRIBER_PROCESS;
                default:
                    return BROKER_PROCESS;
            }
        }
    }
}
