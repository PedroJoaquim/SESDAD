using System;
using System.Collections.Generic;
using Shared_Library;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;
using System.Collections;

namespace Publisher
{
    class Publisher : RemoteEntity, IRemotePublisher
    {
        private const int NUM_THREADS = 10;
        private const int QUEUE_SIZE = 10;

        private PublisherFaultManager fManager;

        public PublisherFaultManager FManager
        {
            get
            {
                return fManager;
            }

            set
            {
                fManager = value;
            }
        }

        public Publisher(String name, String url, String pmUrl) : base(name, url, pmUrl, QUEUE_SIZE, NUM_THREADS)
        {
            this.FManager = new PublisherFaultManager(this);
        }


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

        public void Publish(string topic, int nrEvents, int ms)
        {
            this.Events.Produce(new PublishCommand(topic, nrEvents, ms));
        }


        public override void ReceiveACK(int timeoutID, string entityName, string entitySite)
        {
            this.FManager.ActionACKReceived(timeoutID, entityName, entitySite);
        }

        static void Main(string[] args)
        {
            if (args.Length < 3) return;

            Publisher p = new Publisher(args[0], args[1], args[2]);
            p.Start();
        }

        public override void ConnectionsCreated()
        {
            //ignore
        }
    }
}
