using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shared_Library
{   
    
    public interface ITimeoutListener
    {
        void ActionTimedout(DifundPublishEventProperties properties);
    }



    public abstract class FaultManager : ITimeoutListener
    {
        private TimeoutMonitor tMonitor;
        private RemoteEntity remoteEntity;

        public TimeoutMonitor TMonitor
        {
            get
            {
                return tMonitor;
            }

            set
            {
                tMonitor = value;
            }
        }

        public RemoteEntity RemoteEntity
        {
            get
            {
                return remoteEntity;
            }

            set
            {
                remoteEntity = value;
            }
        }

        public FaultManager(RemoteEntity re)
        {
            this.TMonitor = new TimeoutMonitor(this);
            this.RemoteEntity = re;
        }

        public abstract void ActionACKReceived(int actionID, string entityName);

        public abstract void ActionTimedout(DifundPublishEventProperties properties);

        protected void ExecuteEventTransmissionAsync(Event e, string targetSite, int outSeqNumber, int timeoutID, bool retransmission)
        {
            new Task(() => { SendEvent (e, targetSite, outSeqNumber, timeoutID, retransmission); }).Start();
        }

        private void SendEvent(Event e, string targetSite, int outSeqNumber, int timeoutID, bool retransmission)
        {
            try
            {
                RemoteEntity.RemoteNetwork.ChooseBroker(targetSite, e.Publisher, retransmission).DifundPublishEvent(e, RemoteEntity.RemoteNetwork.SiteName, RemoteEntity.Name, outSeqNumber, timeoutID);
            }
            catch(Exception)
            {
                //ignore 
            }
            
        }
    }

    public class TimeoutMonitor
    {
        private const int SLEEP_TIME = 3000; //miliseconds
        private const int TIMEOUT = 1000; //miliseconds

        private ITimeoutListener mainEntity;
        private int actionsID;

        //actions that the mainEntity performed and is waiting confirmation
        private Dictionary<int, ActionProperties> performedActions = new Dictionary<int, ActionProperties>();

        public ITimeoutListener MainEntity
        {
            get
            {
                return mainEntity;
            }

            set
            {
                mainEntity = value;
            }
        }

        public TimeoutMonitor(ITimeoutListener mainEntity)
        {
            this.MainEntity = mainEntity;
            this.actionsID = 1;
            Thread t = new Thread(MonitorizeTimeOuts);
            t.Start();
        }

        public int NewActionPerformed(Event e, int outSeqNumber, string targetSite)
        {
            int newActionId = IncActionID();

            lock(this)
            {
                this.performedActions.Add(newActionId, new DifundPublishEventProperties(newActionId, targetSite, e, outSeqNumber));
            }

            return newActionId;
        }

        public void PostACK(int actionID)
        {
            lock(this)
            {
                this.performedActions.Remove(actionID);
            }
        }

        private void MonitorizeTimeOuts()
        {
            while(true)
            {
                Thread.Sleep(SLEEP_TIME);

                lock (this)
                {
                    List<int> toBeRemoved = new List<int>();

                    foreach (KeyValuePair<int,  ActionProperties> entry in performedActions)
                    {
                        DateTime now = DateTime.Now;
                        DateTime creation = entry.Value.CreationTime;
                        int diff = (int) ((TimeSpan)(now - creation)).TotalMilliseconds;
                        ActionProperties value = entry.Value;

                        if (diff > TIMEOUT)
                        {
                            Thread t = new Thread(() => PerformTimeoutAlert(value));
                            t.Start();
                            toBeRemoved.Add(entry.Key);
                        }
                    }

                    foreach (int i in toBeRemoved)
                    {
                        this.performedActions.Remove(i);
                    }
                }
            }
        }

        private void PerformTimeoutAlert(ActionProperties ap)
        {
            if (ap.GetType() == typeof(DifundPublishEventProperties))
            {
                DifundPublishEventProperties dp = (DifundPublishEventProperties)ap;
                Console.WriteLine("[TIMEOUT] Event: " + dp.E.Publisher + " #" + dp.E.EventNr);
                this.MainEntity.ActionTimedout(dp);
            }

        }

        private int IncActionID()
        {
            lock(this)
            {
                int id = actionsID;
                this.actionsID++;
                return id;
            }
        }
    }
       
    public abstract class ActionProperties
    {
        private DateTime creationTime;
        private string targetSite;
        private int id;

        #region "properties"
        public DateTime CreationTime
        {
            get
            {
                return creationTime;
            }

            set
            {
                creationTime = value;
            }
        }

        public string TargetSite
        {
            get
            {
                return targetSite;
            }

            set
            {
                targetSite = value;
            }
        }

        public int Id
        {
            get
            {
                return id;
            }

            set
            {
                id = value;
            }
        }
        #endregion

        public ActionProperties(int id, string targetSite)
        {
            this.Id = id;
            this.creationTime = DateTime.Now;
            this.TargetSite = targetSite;
        }
    }

    public class DifundPublishEventProperties : ActionProperties
    {
        private Event e;
        private int outSeqNumber;

        #region "properties"
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

        public int OutSeqNumber
        {
            get
            {
                return outSeqNumber;
            }

            set
            {
                outSeqNumber = value;
            }
        }
        #endregion

        public DifundPublishEventProperties(int id, string targetSite, Event e, int outSeqNumber) : base(id, targetSite)
        {
            this.E = e;
            this.OutSeqNumber = outSeqNumber;
        }
    }
}
