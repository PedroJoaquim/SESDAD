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

namespace Subscriber
{
    class Subscriber : RemoteEntity, IRemoteSubscriber
    {
        private const int NUM_THREADS = 1;
        private const int QUEUE_SIZE = 100;


        private List<Tuple<string, int>> canDeliver = new List<Tuple<string, int>>();
        private List<Tuple<string, int>> authHistory = new List<Tuple<string, int>>();
        private Dictionary<string, List<Event>> waitingEvents = new Dictionary<string, List<Event>>();
        private Dictionary<string, List<int>> receivedEvents = new Dictionary<string, List<int>>();


        public List<Tuple<string, int>> CanDeliver
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

                CanDeliver.Add(new Tuple<string, int>(publisher, eventNr));
                authHistory.Add(new Tuple<string, int>(publisher, eventNr));
            }

            DeliverEvents();
        }

        public override void ReceiveACK(int timeoutID, string entityName, string entitySite)
        {
            //IGNORE
        }



        #endregion

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


        private void DeliverEvents()
        {
            lock (this)
            {
                Event notifyEvent;

                while ((notifyEvent = HaveEventsToDeliver()) != null)
                {
                    PresetEvent(notifyEvent);
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

            Tuple<string, int> canDeliverInfo = CanDeliver.ElementAt(0);

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
            //ignore
        }


    }
}
