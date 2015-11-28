using System;
using System.Collections.Generic;
using System.Threading;
using Shared_Library;
using System.Threading.Tasks;

namespace Shared_Library
{   
    
    public interface ITimeoutListener
    {
        void ActionTimedout(DifundPublishEventProperties properties);
    }



    public abstract class FaultManager : ITimeoutListener
    {
        private const int MAX_MISSED_ACKS = 5;

        private TimeoutMonitor tMonitor;
        private RemoteEntity remoteEntity;
        private Dictionary<string, int> missedACKs;

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
            this.missedACKs = new Dictionary<string, int>();
        }

        public abstract void ActionACKReceived(int actionID, string entityName, string entitySite);

        public abstract void ActionTimedout(DifundPublishEventProperties properties);

        protected string ExecuteEventTransmissionAsync(Event e, string targetSite, int outSeqNumber, int timeoutID, bool retransmission)
        {
            IRemoteBroker targetBroker = RemoteEntity.RemoteNetwork.ChooseBroker(targetSite, e.Publisher, retransmission);
            RemoteEntity.Events.Produce(new ForwardEventCommand(e, targetSite, outSeqNumber, timeoutID, targetBroker));
            return RemoteEntity.RemoteNetwork.GetBrokerName(targetBroker);
        }

        protected bool HasMissedMaxACKs(string siteName)
        {
            lock(missedACKs)
            {
                if (!missedACKs.ContainsKey(siteName))
                    return false;
                else
                    return missedACKs[siteName] >= MAX_MISSED_ACKS;
            }
        }

        protected void IncMissedACKs(string siteName)
        {
            lock (missedACKs)
            {
                if (!missedACKs.ContainsKey(siteName))
                    missedACKs[siteName] = 0;

                missedACKs[siteName] = missedACKs[siteName] + 1;
            }
        }

        protected void ResetMissedACKs(string siteName)
        {
            lock (missedACKs)
            {
                if(!missedACKs.ContainsKey(siteName))
                    missedACKs[siteName] = 0;
                else
                {
                    if(!(missedACKs[siteName] >= MAX_MISSED_ACKS))
                        missedACKs[siteName] = 0;
                }
            }
        }
    }

    public class TimeoutMonitor
    {
        private const int SLEEP_TIME = 1000; //miliseconds
        private const int TIMEOUT = 3000; //miliseconds

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

            return NewActionPerformed(e, outSeqNumber, targetSite, newActionId);
        }

        public int NewActionPerformed(Event e, int outSeqNumber, string targetSite, int timeoutID)
        {
            lock (this)
            {
                this.performedActions.Add(timeoutID, new DifundPublishEventProperties(timeoutID, targetSite, e, outSeqNumber));
            }

            return timeoutID;
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
                            new Task(() => PerformTimeoutAlert(value)).Start();
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
