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
    class Subscriber : MarshalByRefObject, IRemoteSubscriber
    {
        private String name;
        private String url;
        private String pmURL;

        #region "Properties"
        public string Name
        {
            get
            {
                return name;
            }

            set
            {
                name = value;
            }
        }

        public string Url
        {
            get
            {
                return url;
            }

            set
            {
                url = value;
            }
        }

        public string PmURL
        {
            get
            {
                return pmURL;
            }

            set
            {
                pmURL = value;
            }
        }
        #endregion

        public Subscriber(String name, String url, String pmUrl)
        {
            this.Name = name;
            this.Url = url;
            this.PmURL = pmUrl;
        }

        public void Start()
        {
            Register();
            Console.ReadLine();
        }

        private void Register()
        {
            int port = Int32.Parse(Utils.GetIPPort(this.Url));
            string objName = Utils.GetObjName(this.Url);

            TcpChannel chan = new TcpChannel(port);
            ChannelServices.RegisterChannel(chan, false);
            RemotingServices.Marshal(this, objName, typeof(IRemoteSubscriber));

            IRemotePuppetMaster pm = (IRemotePuppetMaster)Activator.GetObject(typeof(IRemotePuppetMaster), this.PmURL);
            pm.RegisterPublisher(this.Url, this.Name);
        }


        static void Main(string[] args)
        {
            if (args.Length < 3) return;

            Subscriber s = new Subscriber(args[0], args[1], args[2]);
            s.Start();
        }
    }
}
