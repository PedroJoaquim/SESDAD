using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;
using System.Collections;
using System.Threading;

namespace Subscriber
{
    class Subscriber : RemoteEntity, IRemoteSubscriber
    {
        private const int NUM_THREADS = 1;
        private const int QUEUE_SIZE = 100;
        private const int SLEEP_MONITOR_TIME = 5000;
        private const int MONITOR_TIME = 15000;

        private List<Tuple<string, int, DateTime>> canDeliver = new List<Tuple<string, int, DateTime>>();
        private List<Tuple<string, int>> authHistory = new List<Tuple<string, int>>();
        private Dictionary<string, List<Event>> waitingEvents = new Dictionary<string, List<Event>>();
        private Dictionary<string, List<int>> receivedEvents = new Dictionary<string, List<int>>();

        private DateTime lastTimeDelivered;

        public List<Tuple<string, int, DateTime>> CanDeliver
        {
            get
            {
                return canDeliver;
            }

            set
            {
                canDeliver = value;
            }
        }

        public Subscriber(String name, String url, String pmUrl) : base(name, url, pmUrl, QUEUE_SIZE, NUM_THREADS) { }


        public override void Register()
        {
            int port = Int32.Parse(Utils.GetIPPort(this.Url));
            string objName = Utils.GetObjName(this.Url);
            BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();

            IDictionary props = new Hashtable();
            props["port"] = port;
            props["timeout"] = SysConfig.REMOTE_CALL_TIMEOUT;

            TcpChannel Channel = new TcpChannel(props, null, provider);
            ChannelServices.RegisterChannel(Channel, false);
            RemotingServices.Marshal(this, objName, typeof(IRemoteSubscriber));

            IRemotePuppetMaster pm = (IRemotePuppetMaster)Activator.GetObject(typeof(IRemotePuppetMaster), this.PmURL);
            pm.RegisterSubscriber(this.Url, this.Name);
            this.PuppetMaster = pm;
        }

        public override void Status()
        {
            Console.WriteLine();
            Console.WriteLine(String.Format("##################### STATUS: {0} #####################", this.Name));
            Console.WriteLine();
            Console.WriteLine(String.Format("*********************** Connections *********************** \r\n"));

            foreach (KeyValuePair<string, IRemoteBroker> entry in this.RemoteNetwork.InBrokers)
            {
                Console.WriteLine(String.Format("[BROKER] {0}", entry.Key));
            }
            foreach (KeyValuePair<string, IRemotePublisher> entry in this.RemoteNetwork.Publishers)
            {
                Console.WriteLine(String.Format("[PUBLISHER] {0}", entry.Key));
            }
            foreach (KeyValuePair<string, IRemoteSubscriber> entry in this.RemoteNetwork.Subscribers)
            {
                Console.WriteLine(String.Format("[SUBSCRIBER] {0}", entry.Key));
            }
            Console.WriteLine(String.Format("###########################################################"));
        }

 

        #region "interface methods"
        public void Subscribe(string topic)
        {
            this.Events.Produce(new SubscribeCommand(topic));
        }

        public void Unsubscribe(string topic)
        {
            this.Events.Produce(new UnsubscribeCommand(topic));
        }

        public void NotifyEvent(Event e)
        {
            if (!ValidEvent(e))
                return;

            if(!SysConfig.Ordering.Equals(SysConfig.TOTAL))
            {
                PresetEvent(e);
                return;
            }


            lock (this)
            {
                if (!waitingEvents.ContainsKey(e.Publisher))
                {
                    waitingEvents[e.Publisher] = new List<Event>();
                }

                waitingEvents[e.Publisher].Add(e);
            }

            DeliverEvents();
        }

        public void SequenceMessage(string publisher, int eventNr)
        {

            lock (this)
            {
                foreach (Tuple<string, int> item in authHistory)
                {
                    if (item.Item1.Equals(publisher) && item.Item2 == eventNr)
                        return;
                }

                CanDeliver.Add(new Tuple<string, int, DateTime>(publisher, eventNr, DateTime.Now));
                authHistory.Add(new Tuple<string, int>(publisher, eventNr));
            }

            DeliverEvents();
        }

        public override void ReceiveACK(int timeoutID, string entityName, string entitySite)
        {
            //IGNORE
        }



        #endregion

        private void MonitorDeadLock()
        {
            while(true)
            {
                Thread.Sleep(SLEEP_MONITOR_TIME);
                lock(this)
                {
                    DateTime now = DateTime.Now;
                    int diff = (int)((TimeSpan)(now - lastTimeDelivered)).TotalMilliseconds;
                    
                    if (waitingEvents.Count > 0 && CanDeliver.Count > 0 && diff > MONITOR_TIME)
                    {
                             
                       while(canDeliver.Count > 0 && !HasMatchingEvent(canDeliver[0].Item1, canDeliver[0].Item2))
                       {
                            int diff2 = (int)((TimeSpan)(now - canDeliver[0].Item3)).TotalMilliseconds;

                            if (diff2 > MONITOR_TIME)
                            {
                                CanDeliver.RemoveAt(0);
                            }
                            else
                            {
                                break;
                            }
                       }

                      DeliverEvents();
                    }
                }
            }
        }


        private bool ContainsEventNumber(List<Event> list, int targetNr)
        {
            foreach (Event item in list)
            {
                if (item.EventNr == targetNr)
                    return true;
            }

            return false;
        }
        private bool ValidEvent(Event e)
        {
            lock (this)
            {
                if (!receivedEvents.ContainsKey(e.Publisher))
                {
                    receivedEvents[e.Publisher] = new List<int>();
                    receivedEvents[e.Publisher].Add(e.EventNr);
                    return true;
                }
                else
                {
                    if (receivedEvents[e.Publisher].Contains(e.EventNr))
                        return false;
                    else
                    {
                        receivedEvents[e.Publisher].Add(e.EventNr);
                        return true;
                    }
                }
            }
        }

        private bool HasMatchingEvent(string publisher, int eventNr)
        {
            if (!waitingEvents.ContainsKey(publisher))
                return false;

            foreach (Event e in waitingEvents[publisher])
            {
                if (e.EventNr == eventNr)
                    return true;
            }

            return false;
        }
        private void DeliverEvents()
        {
            lock (this)
            {
                Event notifyEvent;

                while ((notifyEvent = HaveEventsToDeliver()) != null)
                {
                    PresetEvent(notifyEvent);
                    lastTimeDelivered = DateTime.Now;
                }
            }
        }

        private void PresetEvent(Event notifyEvent)
        {
            PuppetMaster.LogEventDelivery(Name, notifyEvent.Publisher, notifyEvent.Topic, notifyEvent.EventNr);
            Console.WriteLine(String.Format("[EVENT {3}] {1} -----> {0} #{2}", notifyEvent.Topic, notifyEvent.Publisher, notifyEvent.EventNr, Name));
        }

        private Event HaveEventsToDeliver()
        {
            if (CanDeliver.Count == 0)
                return null;

            Tuple<string, int, DateTime> canDeliverInfo = CanDeliver.ElementAt(0);

            if (!waitingEvents.ContainsKey(canDeliverInfo.Item1))
                return null;

            List<Event> targetEvents = waitingEvents[canDeliverInfo.Item1];

            for (int i = 0; i < targetEvents.Count; i++)
            {
                if(targetEvents[i].EventNr == canDeliverInfo.Item2)
                {
                    Event targetEvent = targetEvents[i];
                    targetEvents.RemoveAt(i);
                    CanDeliver.RemoveAt(0);
                    return targetEvent;
                }
            }
            return null;
        }


        static void Main(string[] args)
        {
            if (args.Length < 3) return;

            Subscriber s = new Subscriber(args[0], args[1], args[2]);
            s.Start();
        }

        public override void ConnectionsCreated()
        {
            this.lastTimeDelivered = DateTime.Now;

            if(SysConfig.Ordering.Equals(SysConfig.TOTAL))
            {
                Thread t = new Thread(MonitorDeadLock);
                t.Start();
            }

        }


    }
}
