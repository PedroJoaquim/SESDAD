﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PuppetMaster
{
    public class Logger
    {

        /*
         * Light or Full logging
         */
        private bool fullLogging = false;
        


        public bool IsLogTypeFull()
        {
            return fullLogging;
        }

        public void SetLogTypeFull()
        {
            fullLogging = true;
        }


        public void logCMD(string cmd)
        {
            string output = string.Format("[LOG: {0}] - {1}", GetCurrentTime(), cmd);

            Console.WriteLine(output);

        }

        public void LogEventPublication(string publisher, string topicname, int eventNumber)
        {
            string output = string.Format("[LOG: {0}] - PubEvent {1}, {2}, {3}", GetCurrentTime(), publisher, topicname, eventNumber);

            Console.WriteLine(output);

        }

        public void LogEventForwarding(string broker, string publisher, string topicname, int eventNumber)
        {
            string output = string.Format("[LOG: {0}] - BroEvent {1}, {2}, {3}, {4}", GetCurrentTime(), broker, publisher, topicname, eventNumber);

            Console.WriteLine(output);
        }

        public void LogEventDelivery(string subscriber, string publisher, string topicname, int eventNumber)
        {
            string output = string.Format("[LOG: {0}] - SubEvent {1}, {2}, {3}, {4}", GetCurrentTime(), subscriber, publisher, topicname, eventNumber);

            Console.WriteLine(output);
        }




        public string GetCurrentTime()
        {
            return DateTime.Now.ToString("h:mm:ss");
        }


    }
}
