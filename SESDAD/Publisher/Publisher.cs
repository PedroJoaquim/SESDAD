using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;

namespace Publisher
{
    class Publisher : RemoteEntity, IRemotePublisher
    {
        #region "Properties"
        private int currentEventNr;
        private Dictionary<string, int> topicRelativeNr = new Dictionary<string, int>();


        public int CurrentEventNr
        {
            get
            {
                return currentEventNr;
            }

            set
            {
                currentEventNr = value;
            }
        }
        #endregion

        static void Main(string[] args)
        {
            if (args.Length < 3) return;

            Publisher p = new Publisher(args[0], args[1], args[2]);
            p.Start();
        }

        public Publisher(String name, String url, String pmUrl) : base(name, url, pmUrl)
        {
            this.CurrentEventNr = 1;
        }


        public void ExecuteEventPublication(string topic)
        {
            lock(this)
            {
                Event newEvent = new Event(this.Name, topic, new DateTime().Ticks, this.CurrentEventNr);
                this.PuppetMaster.LogEventPublication(this.Name, newEvent.Topic, newEvent.EventNr); //remote call
                this.Brokers.ElementAt(0).Value.DifundPublishEvent(newEvent, this.Name, newEvent.EventNr); // remote call
                Console.WriteLine("[EVENT] - #" + this.CurrentEventNr);
                this.CurrentEventNr++;
            } 
        }


        public override void Register()
        {
            int port = Int32.Parse(Utils.GetIPPort(this.Url));
            string objName = Utils.GetObjName(this.Url);

            TcpChannel Channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(Channel, false);
            RemotingServices.Marshal(this, objName, typeof(IRemotePublisher));

            IRemotePuppetMaster pm = (IRemotePuppetMaster)Activator.GetObject(typeof(IRemotePuppetMaster), this.PmURL);
            pm.RegisterPublisher(this.Url, this.Name);
            this.PuppetMaster = pm;
        }

        public override void Status()
        {
            Console.WriteLine();
            Console.WriteLine(String.Format("##################### STATUS: {0} #####################", this.Name));
            Console.WriteLine();
            Console.WriteLine(String.Format("*********************** Connections *********************** \r\n"));

            foreach (KeyValuePair<string, IRemoteBroker> entry in this.Brokers)
            {
                Console.WriteLine(String.Format("[BROKER] {0}", entry.Key));
            }
            foreach (KeyValuePair<string, IRemotePublisher> entry in this.Publishers)
            {
                Console.WriteLine(String.Format("[PUBLISHER] {0}", entry.Key));
            }
            foreach (KeyValuePair<string, IRemoteSubscriber> entry in this.Subscribers)
            {
                Console.WriteLine(String.Format("[SUBSCRIBER] {0}", entry.Key));
            }
            Console.WriteLine(String.Format("###########################################################"));
        }

        #region "interface methods"
        public void Publish(string topic, int nrEvents, int ms)
        {
            this.Events.Produce(new PublishCommand(topic, nrEvents, ms));
        }
        #endregion

        public override int NumThreads()
        {
            return 10;
        }

        public override int SizeQueue()
        {
            return 10;
        }
    }
}
