using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;

namespace Subscriber
{
    class Subscriber : RemoteEntity, IRemoteSubscriber
    {

        static void Main(string[] args)
        {
            if (args.Length < 3) return;

            Subscriber s = new Subscriber(args[0], args[1], args[2]);
            s.Start();
        }


        public Subscriber(String name, String url, String pmUrl) : base(name, url, pmUrl) { }


        public override void Register()
        {
            int port = Int32.Parse(Utils.GetIPPort(this.Url));
            string objName = Utils.GetObjName(this.Url);

            Channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(Channel, false);
            RemotingServices.Marshal(this, objName, typeof(IRemoteSubscriber));

            IRemotePuppetMaster pm = (IRemotePuppetMaster)Activator.GetObject(typeof(IRemotePuppetMaster), this.PmURL);
            pm.RegisterSubscriber(this.Url, this.Name);
            this.PuppetMaster = pm;
        }

        public override void Status()
        {
            throw new NotImplementedException();
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
        #endregion
    }
}
