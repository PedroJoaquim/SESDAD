using Shared_Library;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Broker
{
    class ReplicationStorage : ITimeoutListener
    {
        private List<StoredEvent> storedEvents;
        private TimeoutMonitor tMonitor;
        private Broker broker;
        private int hearthBeatTimeoutID;

        public ReplicationStorage(Broker b)
        {
            this.storedEvents = new List<StoredEvent>();
            this.tMonitor = new TimeoutMonitor(this);
            this.broker = b;
        }

        public void StoreNewEvent(Event e, string sourceSite, string sourceEntity, int inSeqNumber)
        {
            lock (this)
            {
                this.storedEvents.Add(new StoredEvent(inSeqNumber, sourceSite, sourceEntity, e));
            }
        }

        public void EventDispatched(int eventNr, string publisher)
        {
            lock (this)
            {
                int index = -1;

                for (int i = 0; i < this.storedEvents.Count; i++)
                {
                    if(this.storedEvents[i].E.EventNr == eventNr && this.storedEvents[i].E.Publisher.Equals(publisher))
                    {
                        index = i;
                        break;
                    }
                }
                
                StoredEvent old = this.storedEvents[index];
                broker.PEventManager.EventDispatchedByMainServer(old);
                this.storedEvents.RemoveAt(index);
                
            }
        }

        public void ActionTimedout(ActionProperties properties)
        {
            if (properties.GetType() == typeof(WaitHearthBeat))
            {
                broker.PEventManager.PublishStoredEvents(this.storedEvents);
            }

            this.storedEvents = new List<StoredEvent>();
        }

        public void WaitHearthBeat()
        {
            this.hearthBeatTimeoutID = tMonitor.HearthBeatAction();
        }

        public void ReceivedHeathBeat()
        {
            this.tMonitor.PostACK(this.hearthBeatTimeoutID);
            WaitHearthBeat();
        }
    }

    public class StoredEvent
    {
        private int inSeqNumber;
        private string sourceSite;
        private string sourceEntity;
        private Event e;

        public int InSeqNumber
        {
            get
            {
                return inSeqNumber;
            }

            set
            {
                inSeqNumber = value;
            }
        }

        public string SourceSite
        {
            get
            {
                return sourceSite;
            }

            set
            {
                sourceSite = value;
            }
        }

        public Event E
        {
            get
            {
                return e;
            }

            set
            {
                e = value;
            }
        }

        public string SourceEntity
        {
            get
            {
                return sourceEntity;
            }

            set
            {
                sourceEntity = value;
            }
        }

        public StoredEvent(int inSeqNumber, string sourceSite, string sourceEntity, Event e)
        {
            this.InSeqNumber = inSeqNumber;
            this.SourceSite = sourceSite;
            this.SourceEntity = sourceEntity;
            this.E = e;
        }
    }
}
