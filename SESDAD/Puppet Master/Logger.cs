using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace PuppetMaster
{
    public class Logger
    {

        /*
         * Light or Full logging
         */
        private bool fullLogging = false;

        private static String LOG_FILE_PATH = @"../../Log/SESDAD.log";



        public bool IsLogTypeFull()
        {
            return fullLogging;
        }

        public void SetLogTypeFull()
        {
            fullLogging = true;
        }


        public void newConnection()
        {
            string output = string.Format("\r\n####################################################################################################\r\n############################    New Connection @ {0}    ############################\r\n####################################################################################################\r\n", GetCurrentTime());
            WriteToFile(output);              
        }

        public void logCMD(string cmd)
        {
            string output = string.Format("[LOG: {0}] - {1}", GetCurrentTime(), cmd);

            WriteToFile(output);

        }

        public void LogEventPublication(string publisher, string topicname, int eventNumber)
        {
            string output = string.Format("[LOG: {0}] - PubEvent {1}, {2}, {3}", GetCurrentTime(), publisher, topicname, eventNumber);

            WriteToFile(output);

        }

        public void LogEventForwarding(string broker, string publisher, string topicname, int eventNumber)
        {
            string output = string.Format("[LOG: {0}] - BroEvent {1}, {2}, {3}, {4}", GetCurrentTime(), broker, publisher, topicname, eventNumber);

            WriteToFile(output);
           
        }

        public void LogEventDelivery(string subscriber, string publisher, string topicname, int eventNumber)
        {
            string output = string.Format("[LOG: {0}] - SubEvent {1}, {2}, {3}, {4}", GetCurrentTime(), subscriber, publisher, topicname, eventNumber);

            WriteToFile(output);
          
        }

        public string GetCurrentTime()
        {
            return DateTime.Now.ToString("HH:mm:ss | dd/MM/yy");
        }

        public void WriteToFile(string line)
        {
            lock (this)
            {
                StreamWriter file = null;

                try
                {
                    file = new StreamWriter(LOG_FILE_PATH, true);
                    file.WriteLine(line);
                } catch (Exception e)
                {
                    Console.WriteLine("[LOG] Writing to log file failed (exception: {0}", e.Message);
                }
                finally
                {
                    if (file != null) file.Close();
                }
            }
        }

    }
}
