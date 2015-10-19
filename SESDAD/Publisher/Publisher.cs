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

        static void Main(string[] args)
        {
            if (args.Length < 3) return;

            Publisher p = new Publisher(args[0], args[1], args[2]);
            p.Start();
        }

        public Publisher(String name, String url, String pmUrl) : base(name, url, pmUrl) { }

        public override void Register()
        {
            int port = Int32.Parse(Utils.GetIPPort(this.Url));
            string objName = Utils.GetObjName(this.Url);

            TcpChannel chan = new TcpChannel(port);
            ChannelServices.RegisterChannel(chan, false);
            RemotingServices.Marshal(this, objName, typeof(IRemotePublisher));

            IRemotePuppetMaster pm = (IRemotePuppetMaster)Activator.GetObject(typeof(IRemotePuppetMaster), this.PmURL);
            pm.RegisterPublisher(this.Url, this.Name);
        }

        public override void Status()
        {
            throw new NotImplementedException();
        }

        public void Publish(string topic, int nrEvents, int ms)
        {
            throw new NotImplementedException();
        }


        #region "interface methods"

        #endregion
    }
}
