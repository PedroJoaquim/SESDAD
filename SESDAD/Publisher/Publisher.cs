using System;
using System.Collections.Generic;
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

        public Publisher(String name, String url, String pmUrl) : base(name, url, pmUrl, 10, 10)
        {
            this.CurrentEventNr = 1;
        }


        public void ExecuteEventPublication(string topic)
        {
            CheckFreeze();

            lock(this)
            {
                Event newEvent = new Event(this.Name, topic, new DateTime().Ticks, this.CurrentEventNr);
                this.PuppetMaster.LogEventPublication(this.Name, newEvent.Topic, newEvent.EventNr); //remote call
                this.RemoteNetwork.ChooseBroker(RemoteNetwork.SiteName, Name, false).DifundPublishEvent(newEvent, RemoteNetwork.SiteName, this.Name, newEvent.EventNr, -1); // remote call TODO CHANGEME
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
        public void Publish(string topic, int nrEvents, int ms)
        {
            this.Events.Produce(new PublishCommand(topic, nrEvents, ms));
        }

        public override void ActionTimedout(DifundPublishEventProperties p)
        {
            CheckFreeze();

            this.RemoteNetwork.ChooseBroker(RemoteNetwork.SiteName, Name, true).DifundPublishEvent(p.E, RemoteNetwork.SiteName, this.Name, p.E.EventNr, -1);
            Console.WriteLine("[EVENT] - #" + p.E.EventNr + " RETRANSMITED ");
        }
        #endregion
    }
}
