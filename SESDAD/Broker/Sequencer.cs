using Shared_Library;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Broker
{

    class Sequencer
    {
        public const string SEQUENCER_BASE_NAME = "#sequencer#";
        public const string SEQUENCER_PASSIVE_NAME = "#sequencer-passive#";

        private int nextSeqNumber;
        private Broker broker;
        private Dictionary<string, List<int>> dispatchedNewEventMessages;
        private Dictionary<string, List<int>> processedTotalOrderMessages;

        private Dictionary<string, List<Tuple<int, string>>> unprocessedTotalOrderMessages; //for sequencer replica store messages not send
        private Dictionary<string, List<int>> totalOrderMessagesACKs; // messages processed by main sequencer publisher -> eventNr Topic

        public Sequencer(Broker broker)
        {
            this.broker = broker;
            this.nextSeqNumber = 1;
            this.dispatchedNewEventMessages = new Dictionary<string, List<int>>(); //publisher -> eventNr
            this.processedTotalOrderMessages = new Dictionary<string, List<int>>();
            this.unprocessedTotalOrderMessages = new Dictionary<string, List<Tuple<int, string>>>();
            this.totalOrderMessagesACKs = new Dictionary<string, List<int>>();
        }

        public void ProcessedTotalOrderMessage(string publisher, int eventNr)
        {
            lock (this)
            {
                if (!processedTotalOrderMessages.ContainsKey(publisher))
                    processedTotalOrderMessages[publisher] = new List<int>();

                processedTotalOrderMessages[publisher].Add(eventNr);
            }
        }

        public void DispatchedNewEventMessage(string publisher, int eventNr, string topic)
        {
            lock(this)
            {
                if (!dispatchedNewEventMessages.ContainsKey(publisher))
                    dispatchedNewEventMessages[publisher] = new List<int>();

                dispatchedNewEventMessages[publisher].Add(eventNr);

                if(broker.SysConfig.ParentSite.Equals(SysConfig.NO_PARENT))
                {
                    if(!ACKAlreadyReceivedFor(publisher, eventNr))
                    {
                        if (!unprocessedTotalOrderMessages.ContainsKey(publisher))
                            unprocessedTotalOrderMessages[publisher] = new List<Tuple<int, string>>();

                        unprocessedTotalOrderMessages[publisher].Add(new Tuple<int, string>(eventNr, topic));
                    }
                }
            }
        }

        public void ACKForSequencerEventMessage(string publisher, int eventNr)
        {
            if (!totalOrderMessagesACKs.ContainsKey(publisher))
                totalOrderMessagesACKs[publisher] = new List<int>();

            totalOrderMessagesACKs[publisher].Add(eventNr);

            if(unprocessedTotalOrderMessages.ContainsKey(publisher))
            {
                List<Tuple<int, string>> targetList = unprocessedTotalOrderMessages[publisher];

                for (int i = 0; i < targetList.Count; i++)
                {
                   if(targetList[i].Item1 == eventNr)
                    {
                        targetList.RemoveAt(i);
                        return;
                    } 
                }
            }
        }

        //check if main sequencer already has dispatched that message
        public bool ACKAlreadyReceivedFor(string publisher, int eventNR)
        {
            return totalOrderMessagesACKs.ContainsKey(publisher) &&
                   totalOrderMessagesACKs[publisher].Contains(eventNR);
        }

        public bool AlreadyDispatchedNewEventMessage(string publisher, int eventNr)
        {
            lock(this)
            {
                if (!dispatchedNewEventMessages.ContainsKey(publisher))
                    return false;
                else
                    return dispatchedNewEventMessages[publisher].Contains(eventNr);
            }
        }

        public bool AlreadyProcessedTotalOrderMessage(string publisher, int eventNr)
        {
            lock (this)
            {
                if (!processedTotalOrderMessages.ContainsKey(publisher))
                    return false;
                else
                    return processedTotalOrderMessages[publisher].Contains(eventNr);
            }
        }
       
        public int GetNextSeqNumber()
        {
            lock(this)
            {
                int result = nextSeqNumber;
                this.nextSeqNumber++;
                return result;
            }
        }

        public void DifundUnprocessedMessages()
        {

            foreach (KeyValuePair<string, List<Tuple<int, string>>> item in unprocessedTotalOrderMessages)
            {
                foreach (Tuple<int, string> eventInfo in item.Value)
                {
                    lock (this)
                    {
                        if (AlreadyDispatchedNewEventMessage(item.Key, eventInfo.Item1))
                            continue;
                        else
                            DispatchedNewEventMessage(item.Key, eventInfo.Item1, eventInfo.Item2);
                    }

                    broker.Events.Produce(new TotalOrderNewEventCommand(eventInfo.Item2, item.Key, eventInfo.Item1));
                }
            }

            this.unprocessedTotalOrderMessages = new Dictionary<string, List<Tuple<int, string>>>();
        }


        public void CheckIfSequencer()
        {
            int myCode = broker.Name.GetHashCode();

            foreach (KeyValuePair<string, IRemoteBroker> item in broker.RemoteNetwork.InBrokers)
            {
                if (item.Key.GetHashCode() > myCode)
                {
                    broker.IsSequencer = false;
                    broker.IsPassiveSequencer = false;
                    return;
                }
            }
            Console.WriteLine("[SEQUENCER]");
            broker.IsSequencer = true;
            broker.IsPassiveSequencer = false;
        }

        public bool CheckIfPassiveSequencer()
        {
            int myCode = broker.Name.GetHashCode();
            int myPassiveCode = broker.SysConfig.PassiveServer.GetHashCode();
            int otherCode = -1;

            foreach (KeyValuePair<string, IRemoteBroker> item in broker.RemoteNetwork.InBrokers)
            {
                if (!item.Key.Equals(broker.SysConfig.PassiveServer))
                {
                    otherCode = item.Key.GetHashCode();
                }
            }


            if (otherCode > myCode && otherCode > myPassiveCode)
            {
                broker.IsPassiveSequencer = true;
                Console.WriteLine("[INFO] Elected as new Sequencer");

                foreach (IRemoteBroker item in broker.RemoteNetwork.InBrokersList)
                {
                    try { item.DisableSequencer(); } catch(Exception) { /*ignore*/ }
                }


                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
