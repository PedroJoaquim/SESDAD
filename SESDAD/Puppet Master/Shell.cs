using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace PuppetMaster
{
    class Shell
    {

        #region "attrs"
        private SystemNetwork network;
        private Logger log;

        internal SystemNetwork Network
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

        public Logger Log
        {
            get
            {
                return log;
            }

            set
            {
                log = value;
            }
        }
        #endregion

        #region "string literals"
        private const string SUBSCRIBE = "subscribe";
        private const string UNSUBSCRIBE = "unsubscribe";
        private const string SUBSCRIBER = "subscriber";
        private const string PUBLISHER = "publisher";
        private const string PUBLISH = "publish";
        private const string ON_TOPIC = "ontopic";
        private const string INTERVAL = "interval";
        private const string STATUS = "status";
        private const string CRASH = "crash";
        private const string FREEZE = "freeze";
        private const string UNFREEZE = "unfreeze";
        private const string WAIT = "wait";
        private const string EXIT = "exit";

        private const string SPACE = @"[ /t]";
        private const string NAME = @"[a-zA-Z0-9]+";
        private const string TOPIC_REGEX = @"(\/)?(([a-zA-Z0-9]+\/)+|\*\/)*([a-zA-Z0-9]+(\/)?|\*(\/)?)";
        private const string POSITIVE_NUMBERS = @"[1-9][0-9]*";
        #endregion

        private const string SUBSCRIBE_CMD_REGEX = SUBSCRIBER + SPACE + NAME + SPACE + SUBSCRIBE + "|" + UNSUBSCRIBE + SPACE + TOPIC_REGEX;
        private const string PUBLISH_CMD_REGEX = PUBLISHER + SPACE + NAME + SPACE + PUBLISH + SPACE + POSITIVE_NUMBERS + SPACE + ON_TOPIC + SPACE + TOPIC_REGEX + SPACE + INTERVAL + SPACE + POSITIVE_NUMBERS;
        private const string STATUS_CMD_REGEX = STATUS;
        private const string CRASH_CMD_REGEX = CRASH + SPACE + NAME;
        private const string FREEZE_CMD_REGEX = FREEZE + SPACE + NAME;
        private const string UNFREEZE_CMD_REGEX = UNFREEZE + SPACE + NAME;
        private const string WAIT_CMD_REGEX = WAIT + SPACE + POSITIVE_NUMBERS;



        public Shell(SystemNetwork network, Logger log)
        {
            Network = network;
            Log = log;
        }


        public void ProcessCommand(String cmd, int lineNr = -1)
        {

            string[] splitedCMD = cmd.ToLower().Split(' ');

            if (cmd.Trim().Length == 0) return;

            switch (splitedCMD[0])
            {
                case SUBSCRIBER:
                    processSubscriberCommand(splitedCMD, cmd);
                    break;

                case PUBLISHER:
                    processPublisherCommand(splitedCMD, cmd);
                    break;

                case STATUS:
                    processStatusCommand();
                    break;

                case CRASH:
                    processCrashCommand(splitedCMD, cmd);
                    break;

                case FREEZE:
                    processFreezeCommand(splitedCMD, cmd);
                    break;

                case UNFREEZE:
                    processUnfreezeCommand(splitedCMD, cmd);
                    break;

                case WAIT:
                    processWaitCommand(splitedCMD, cmd);
                    break;

                case EXIT:
                    break;
                default:
                    if (lineNr != -1)
                        Console.WriteLine("[ERROR] Unknown command at line: {0}", lineNr);
                    else
                        Console.WriteLine("[ERROR] Unknown command");
                    break;
            }
        }

        private void validateCMD(string cmd, string regex, string cmdType)
        {
            Regex rgx = new Regex(regex, RegexOptions.IgnoreCase);

            if (cmd == null || !rgx.IsMatch(cmd))
            {
                throw new Exception("Invalid syntax for command [" + cmdType + "]");
            }

        }


        private void processWaitCommand(string[] splitedCMD, string cmd)
        {
            validateCMD(cmd, WAIT_CMD_REGEX, splitedCMD[0]);

            int ms = Int32.Parse(splitedCMD[1]);
            Thread.Sleep(ms);

            Log.logCMD(cmd);
        }

        private void processUnfreezeCommand(string[] splitedCMD, string cmd)
        {
            validateCMD(cmd, UNFREEZE_CMD_REGEX, splitedCMD[0]);

            string processName = splitedCMD[1];
            Entity entity = Network.GetEntity(processName);
            entity.GetRemoteEntity().Unfreeze();

            Log.logCMD(cmd);
        }

        private void processFreezeCommand(string[] splitedCMD, string cmd)
        {
            validateCMD(cmd, FREEZE_CMD_REGEX, splitedCMD[0]);

            string processName = splitedCMD[1];
            Entity entity = Network.GetEntity(processName);
            entity.GetRemoteEntity().Freeze();

            Log.logCMD(cmd);
        }

        private void processCrashCommand(string[] splitedCMD, string cmd)
        {
            validateCMD(cmd, CRASH_CMD_REGEX, splitedCMD[0]);

            string processName = splitedCMD[1];
            Entity entity = Network.GetEntity(processName);

            try
            {
                entity.GetRemoteEntity().Crash();
            }
            catch (Exception)
            {
            }
            
            Log.logCMD(cmd);
        }

        private void processStatusCommand()
        {
            foreach (KeyValuePair<string, Entity> entry in Network.Entities)
            {
                entry.Value.GetRemoteEntity().Status();
            }
        }

        private void processPublisherCommand(string[] splitedCMD, string cmd)
        {
            validateCMD(cmd, PUBLISH_CMD_REGEX, splitedCMD[0]);

            string processName = splitedCMD[1];
            int numberOfEvents = Int32.Parse(splitedCMD[3]);
            string topicName = splitedCMD[5];
            int ms = Int32.Parse(splitedCMD[7]);

            PublisherEntity entity = (PublisherEntity)Network.GetEntity(processName);
            entity.RemoteEntity.Publish(topicName, numberOfEvents, ms);

            Log.logCMD(cmd);
        }

        private void processSubscriberCommand(string[] splitedCMD, string cmd)
        {
            validateCMD(cmd, SUBSCRIBE_CMD_REGEX, splitedCMD[0]);

            string processName = splitedCMD[1];
            string operation = splitedCMD[2];
            string topicName = splitedCMD[3];

            SubscriberEntity entity = (SubscriberEntity)Network.GetEntity(processName);

            if (operation.Equals(SUBSCRIBE))
            {
                entity.RemoteEntity.Subscribe(topicName);
            }
            else
            {
                entity.RemoteEntity.Unsubscribe(topicName);
            }

            Log.logCMD(cmd);
        }
    }
}
