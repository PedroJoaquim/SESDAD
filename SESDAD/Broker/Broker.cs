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
        #endregion


        public Broker(String name, String url, String pmUrl) : base(name, url, pmUrl) { }

        public override void Register()
        {
            int port = Int32.Parse(Utils.GetIPPort(this.Url));
            string objName = Utils.GetObjName(this.Url);

            Channel= new TcpChannel(port);
            ChannelServices.RegisterChannel(Channel, false);
            RemotingServices.Marshal(this, objName, typeof(IRemoteBroker));

            IRemotePuppetMaster pm = (IRemotePuppetMaster)Activator.GetObject(typeof(IRemotePuppetMaster), this.PmURL);
            pm.RegisterBroker(this.Url, this.Name);
            this.PuppetMaster = pm;
        }

        public override void Status()
        {
            throw new NotImplementedException();
        }

        #region "interface methods"
        public void DifundPublishEvent(Event e, string source)
        {
            this.Events.Produce(new DifundPublishEventCommand(e, source));
        }

        public void DifundSubscribeEvent(string topic, string source)
        {
            Console.WriteLine("evento recebido");
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
    }
}
