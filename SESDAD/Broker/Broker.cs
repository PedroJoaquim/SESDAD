﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;
using System.Collections;

namespace Broker 
{
    class Broker : RemoteEntity, IRemoteBroker
    {
        private const int QUEUE_SIZE = 500;
        private const int NUM_THREADS = 200;

        private ForwardingTable forwardingTable = new ForwardingTable();
        private ReceiveTable receiveTable = new ReceiveTable();
        private PublishEventManager pEventManager;
        private BrokerFaultManager fManager;
        private ReplicationStorage repStorage;
        private Sequencer sequencer;
        private bool isSequencer;
        private bool isPassiveSequencer;

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

        public BrokerFaultManager FManager
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

        public ReplicationStorage RepStorage
        {
            get
            {
                return repStorage;
            }

            set
            {
                repStorage = value;
            }
        }

        public bool IsSequencer
        {
            get
            {
                return isSequencer;
            }

            set
            {
                isSequencer = value;
            }
        }

        internal Sequencer Sequencer
        {
            get
            {
                return sequencer;
            }

            set
            {
                sequencer = value;
            }
        }

        public bool IsPassiveSequencer
        {
            get
            {
                return isPassiveSequencer;
            }

            set
            {
                isPassiveSequencer = value;
            }
        }
        #endregion

        public Broker(String name, String url, String pmUrl) : base(name, url, pmUrl, QUEUE_SIZE, NUM_THREADS)
        {

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
            RemotingServices.Marshal(this, objName, typeof(IRemoteBroker));

            IRemotePuppetMaster pm = (IRemotePuppetMaster)Activator.GetObject(typeof(IRemotePuppetMaster), this.PmURL);
            pm.RegisterBroker(this.Url, this.Name);
            this.PuppetMaster = pm;

            if (this.SysConfig.Ordering.Equals(SysConfig.FIFO))
                this.PEventManager = new FIFOPublishEventManager(this);

            if (this.SysConfig.Ordering.Equals(SysConfig.NO_ORDER))
                this.PEventManager = new NoOrderPublishEventManager(this);

            if (this.SysConfig.Ordering.Equals(SysConfig.TOTAL))
                this.PEventManager = new TotalOrderPublishEventManager(this);

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
            this.Events.Produce(new DifundPublishEventCommand(e, sourceSite, sourceEntity, seqNumber));
            FManager.Events.Produce(new ReplicateCommand(FManager.PassiveServer, e, sourceSite, sourceEntity, seqNumber, timeoutID));
        }

        public void DifundSubscribeEvent(string topic, string source)
        {
            this.Events.Produce(new DifundSubscribeEventCommand(topic, source));
        }

        public void DifundUnSubscribeEvent(string topic, string source)
        {
            this.Events.Produce(new DifundUnSubscribeEventCommand(topic, source));
        }

        public void DifundSequencerMessage(Event e, string sourceSite, string sourceEntity, int seqNumber)
        {
            lock (Sequencer)
            {
                if (Sequencer.AlreadyProcessedTotalOrderMessage(e.Publisher, e.EventNr))
                    return;
                else
                    Sequencer.ProcessedTotalOrderMessage(e.Publisher, e.EventNr);
            }

            this.Events.Produce(new DifundPublishEventCommand(e, sourceSite, sourceEntity, seqNumber));
        }

        public void NewEventPublished(string topic, string publisher, int eventNr)
        {
            lock(Sequencer)
            {
                if (Sequencer.AlreadyDispatchedNewEventMessage(publisher, eventNr))
                    return;
                else
                    Sequencer.DispatchedNewEventMessage(publisher, eventNr, topic);
            }

            this.Events.Produce(new TotalOrderNewEventCommand(topic, publisher, eventNr));
        }
        #endregion

        static void Main(string[] args)
        {
            if (args.Length < 3) return;

            Broker b = new Broker(args[0], args[1], args[2]);
            b.Start();
        }

        public override void ReceiveACK(int timeoutID, string entityName, string entitySite)
        {
            this.FManager.ActionACKReceived(timeoutID, entityName, entitySite);
        }


        /*
         *  Passive redundancy
         */

        public void StoreNewEvent(Event e, string sourceSite, string sourceEntity, int inSeqNumber)
        {
            CheckFreeze();
            this.RepStorage.StoreNewEvent(e, sourceSite, sourceEntity, inSeqNumber);
        }

        public void EventDispatched(int eventNr, string publisher)
        {
            CheckFreeze();
            this.RepStorage.EventDispatched(eventNr, publisher);
        }

        public void HearthBeat()
        {
            this.RepStorage.ReceivedHeathBeat();
        }

        public override void ConnectionsCreated()
        {
            Console.WriteLine("[PASSIVE SERVER] " + this.SysConfig.PassiveServer);
            this.FManager = new BrokerFaultManager(this);
            this.RepStorage = new ReplicationStorage(this);
            this.FManager.PassiveServer = RemoteNetwork.GetBrokerByName(this.SysConfig.PassiveServer);
            this.Sequencer = new Sequencer(this);

            if (SysConfig.ParentSite.Equals(SysConfig.NO_PARENT))
            {
                Sequencer.CheckIfSequencer();
            }
            else
            {
                IsSequencer = false;
                IsPassiveSequencer = false;
            }

            this.RepStorage.WaitHearthBeat();
        }

        public void SequencerEventDispatched(int eventNr, string publisher)
        {
            this.Sequencer.ACKForSequencerEventMessage(publisher, eventNr);
        }

        public void DisableSequencer()
        {
            this.IsSequencer = false;
        }
    }
}
