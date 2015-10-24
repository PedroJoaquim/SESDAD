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


        private int ConcurrentEventNumberGetter(int nrEvents)
        {
            int result;

            lock (this)
            {
                result = this.CurrentEventNr;
                this.CurrentEventNr = result + nrEvents;
            }

            return result;
        }


        public override void Register()
        {
            int port = Int32.Parse(Utils.GetIPPort(this.Url));
            string objName = Utils.GetObjName(this.Url);

            Channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(Channel, false);
            RemotingServices.Marshal(this, objName, typeof(IRemotePublisher));

            IRemotePuppetMaster pm = (IRemotePuppetMaster)Activator.GetObject(typeof(IRemotePuppetMaster), this.PmURL);
            pm.RegisterPublisher(this.Url, this.Name);
            this.PuppetMaster = pm;
        }

        public override void Status()
        {
            throw new NotImplementedException();
        }

        #region "interface methods"
        public void Publish(string topic, int nrEvents, int ms)
        {
            int eventNumber = ConcurrentEventNumberGetter(nrEvents);
            this.Events.Produce(new PublishCommand(topic, nrEvents, ms, eventNumber));
        }
        #endregion
    }
}
