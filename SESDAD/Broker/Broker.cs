using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;

namespace Broker 
{
    class Broker : RemoteEntity, IRemoteBroker
    {

        private ForwardingTable forwardingTable = new ForwardingTable();
        private ReceiveTable receiveTable = new ReceiveTable();
        private PublishEventManager pEventManager;
        private FaultManager fManager;

        #region "properties"
        public ForwardingTable ForwardingTable
        {
            get
            {
                return forwardingTable;
            }

            set
            {
                forwardingTable = value;
            }
        }

        internal ReceiveTable ReceiveTable
        {
            get
            {
                return receiveTable;
            }

            set
            {
                receiveTable = value;
            }
        }

        internal PublishEventManager PEventManager
        {
            get
            {
                return pEventManager;
            }

            set
            {
                pEventManager = value;
            }
        }

        internal FaultManager FManager
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
        #endregion

        public Broker(String name, String url, String pmUrl) : base(name, url, pmUrl, 300, 20) { }

        public override void Register()
        {
            int port = Int32.Parse(Utils.GetIPPort(this.Url));
            string objName = Utils.GetObjName(this.Url);

            TcpChannel Channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(Channel, false);
            RemotingServices.Marshal(this, objName, typeof(IRemoteBroker));

            IRemotePuppetMaster pm = (IRemotePuppetMaster)Activator.GetObject(typeof(IRemotePuppetMaster), this.PmURL);
            pm.RegisterBroker(this.Url, this.Name);
            this.PuppetMaster = pm;

            if (this.SysConfig.Ordering.Equals(SysConfig.FIFO))
                this.PEventManager = new FIFOPublishEventManager();

            if (this.SysConfig.Ordering.Equals(SysConfig.NO_ORDER))
                this.PEventManager = new NoOrderPublishEventManager();

            if (this.SysConfig.Ordering.Equals(SysConfig.TOTAL))
                this.PEventManager = new TotalOrderPublishEventManager();

            this.FManager = new FaultManager(this);
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

            foreach (KeyValuePair<string, List<IRemoteBroker>> entry in this.RemoteNetwork.OutBrokers)
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

            Console.WriteLine();
            Console.WriteLine(String.Format("********************* Forwarding Table ********************"));
            this.ForwardingTable.PrintStatus();
            Console.WriteLine();
            Console.WriteLine(String.Format("********************** Receive Table **********************\r\n"));
            this.ReceiveTable.PrintStatus();
            Console.WriteLine(String.Format("###########################################################"));
        }

        #region "interface methods"
        public void DifundPublishEvent(Event e, string sourceSite, string sourceEntity, int seqNumber, int timeoutID)
        {
            this.Events.Produce(new DifundPublishEventCommand(e, sourceSite, seqNumber));
            this.RemoteNetwork.OutBrokersNames[sourceEntity].SendACK(timeoutID);
        }

        public void DifundSubscribeEvent(string topic, string source)
        {
            this.Events.Produce(new DifundSubscribeEventCommand(topic, source));
        }

        public void DifundUnSubscribeEvent(string topic, string source)
        {
            this.Events.Produce(new DifundUnSubscribeEventCommand(topic, source));
        }
        #endregion

        static void Main(string[] args)
        {
            if (args.Length < 3) return;

            Broker b = new Broker(args[0], args[1], args[2]);
            b.Start();
        }

        public override void ActionTimedout(DifundPublishEventProperties properties)
        {
            //ignore faultManager will handle this
        }
    }
}
