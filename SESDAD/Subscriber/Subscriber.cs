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

        private Dictionary<string, List<int>> receivedEvents = new Dictionary<string, List<int>>();

        public Dictionary<string, List<int>> ReceivedEvents
        {
            get
            {
                return receivedEvents;
            }

            set
            {
                receivedEvents = value;
            }
        }

        public Subscriber(String name, String url, String pmUrl) : base(name, url, pmUrl, 100, 1) { }


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
            this.Events.Produce(new NotifyEvent(e));
        }

        internal bool ValidEvent(Event e)
        {
            lock(this)
            {
                if(!ReceivedEvents.ContainsKey(e.Publisher))
                {
                    ReceivedEvents[e.Publisher] = new List<int>();
                    ReceivedEvents[e.Publisher].Add(e.EventNr);
                    return true;
                }
                else
                {
                    if (ReceivedEvents[e.Publisher].Contains(e.EventNr))
                        return false;
                    else
                    {
                        ReceivedEvents[e.Publisher].Add(e.EventNr);
                        return true;
                    }
                }
            }
        }

        public override void ReceiveACK(int timeoutID, string entityName, string entitySite)
        {
            //IGNORE
        }

        #endregion


        static void Main(string[] args)
        {
            if (args.Length < 3) return;

            Subscriber s = new Subscriber(args[0], args[1], args[2]);
            s.Start();
        }
    }
}
