using System;
using System.Collections.Generic;
using System.Threading;
using Shared_Library;
using System.Threading.Tasks;

namespace Shared_Library
{   
    
    public interface ITimeoutListener
    {
        void ActionTimedout(ActionProperties properties);
    }

    public abstract class FaultManager : ITimeoutListener
    {
        private const int MAX_MISSED_ACKS = 3;
        private const int MIN_TIME_BETWEEN_ACKS = 2100;

        private TimeoutMonitor tMonitor;
        private EventQueue events;
        private RemoteEntity remoteEntity;
        private Dictionary<string, Dictionary <string, Pair<DateTime, int>>> missedACKs;

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

        public EventQueue Events
        {
            get
            {
                return events;
            }

            set
            {
                events = value;
            }
        }

        public FaultManager(RemoteEntity re, int queueSize, int numThreads)
        {
            this.TMonitor = new TimeoutMonitor(this);
            this.RemoteEntity = re;
            this.missedACKs = new Dictionary<string, Dictionary<string, Pair<DateTime, int>>>();
            this.Events = new EventQueue(queueSize);

            for (int i = 0; i < numThreads; i++)
            {
                Thread t = new Thread(ProcessQueue);
                t.Start();
            }
        }

        public abstract void ActionACKReceived(int actionID, string entityName, string entitySite);
        public abstract void ActionTimedout(ActionProperties properties);


        private void ProcessQueue()
        {
            Command command;

            while (true)
            {
                command = Events.Consume();
                RemoteEntity.CheckFreeze();
                command.Execute(RemoteEntity);
            }
        }

        public bool HasMissedMaxACKs(string siteName, string entityName)
        {
            lock(missedACKs)
            {
                return missedACKs.ContainsKey(siteName)                           &&
                       missedACKs[siteName].ContainsKey(entityName)               &&
                       missedACKs[siteName][entityName].Second >= MAX_MISSED_ACKS;
            }
        }

        protected void IncMissedACKs(string siteName, string entityName)
        {
            lock (missedACKs)
            {
                if (!missedACKs.ContainsKey(siteName))
                {
                    missedACKs[siteName] = new Dictionary<string, Pair<DateTime, int>>();
                    missedACKs[siteName][entityName] = new Pair<DateTime, int>(DateTime.Now, 1);

                }
                else if (!missedACKs[siteName].ContainsKey(entityName))
                {
                    missedACKs[siteName][entityName] = new Pair<DateTime, int>(DateTime.Now, 1);
                }
                else
                {
                    Pair<DateTime, int> pair = missedACKs[siteName][entityName];
                    DateTime now = DateTime.Now;
                    DateTime lastACK = pair.First;
                    int diff = (int)((TimeSpan)(now - lastACK)).TotalMilliseconds;

                    if(diff > MIN_TIME_BETWEEN_ACKS)
                    {
                        missedACKs[siteName][entityName] = new Pair<DateTime, int>(DateTime.Now, pair.Second+1);
                    }
                }
            } 
        }

        protected void ResetMissedACKs(string siteName, string entityName)
        {
            lock (missedACKs)
            {
                if (!missedACKs.ContainsKey(siteName))
                    return;

                if (!missedACKs[siteName].ContainsKey(entityName))
                    return;

                if (missedACKs[siteName][entityName].Second < MAX_MISSED_ACKS)
                {
                    missedACKs[siteName][entityName].Second = 0;
                }
            }
        }


        public IRemoteBroker ChooseBroker(string site, string publisher)
        {
            RemoteNetwork rn = RemoteEntity.RemoteNetwork;
            List<IRemoteBroker> brokers = site.Equals(rn.SiteName) ? rn.InBrokersList : rn.OutBrokers[site];
            int firstIndex = Utils.CalcBrokerForwardIndex(brokers.Count, publisher, false);
            int secondIndex = Utils.CalcBrokerForwardIndex(brokers.Count, publisher, true);
            string firstBrokerName = rn.GetBrokerName(brokers[firstIndex]);

            if (HasMissedMaxACKs(site, firstBrokerName))
                return brokers[secondIndex];
            else
                return brokers[firstIndex];

        }
    }

    public class TimeoutMonitor
    {
        private const int SLEEP_TIME = 500; //miliseconds

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

        public int NewActionPerformed(Event e, int outSeqNumber, string targetSite, string targetEntity)
        {
            int newActionId = IncActionID();

            return NewActionPerformed(e, outSeqNumber, targetSite, targetEntity, newActionId);
        }

        public int NewActionPerformed(Event e, int outSeqNumber, string targetSite, string targetEntity, int timeoutID)
        {
            lock (this)
            {
                this.performedActions.Add(timeoutID, new DifundPublishEventProperties(timeoutID, targetSite, targetEntity, e, outSeqNumber));
            }

            return timeoutID;
        }

        public int HearthBeatAction()
        {
            int timeoutID = IncActionID();

            lock (this)
            {
                this.performedActions.Add(timeoutID, new WaitHearthBeat(timeoutID));
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

                        if (diff > value.Timeout)
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
                this.MainEntity.ActionTimedout(dp);
            }
            else if (ap.GetType() == typeof(WaitHearthBeat))
            {
                WaitHearthBeat sp = (WaitHearthBeat)ap;
                this.MainEntity.ActionTimedout(sp);
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
        private int id;
        private int timeout;

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

        public int Timeout
        {
            get
            {
                return timeout;
            }

            set
            {
                timeout = value;
            }
        }
        #endregion

        public ActionProperties(int id, int timeout)
        {
            this.Id = id;
            this.Timeout = timeout;
            this.creationTime = DateTime.Now;
        }
    }

    public class DifundPublishEventProperties : ActionProperties
    {

        private const int TIMEOUT = 1500;

        private Event e;
        private int outSeqNumber;
        private string targetSite;
        private string targetEntity;

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

        public string TargetEntity
        {
            get
            {
                return targetEntity;
            }

            set
            {
                targetEntity = value;
            }
        }
        #endregion

        public DifundPublishEventProperties(int id, string targetSite, string targetEntity, Event e, int outSeqNumber) : base(id, TIMEOUT)
        {
            this.E = e;
            this.OutSeqNumber = outSeqNumber;
            this.TargetEntity = targetEntity;
            this.TargetSite = targetSite;
        }
    }

    public class WaitHearthBeat : ActionProperties
    {
        private const int TIMEOUT = 5000;

        public WaitHearthBeat(int id) : base(id, TIMEOUT)   { }
    }
}
