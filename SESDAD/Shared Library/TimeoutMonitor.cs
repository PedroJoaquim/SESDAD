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
        void ActionACKReceived(int actionID);
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
                if (this.performedActions.ContainsKey(actionID))
                    this.performedActions.Remove(actionID);
            }

            MainEntity.ActionACKReceived(actionID);
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
                  
                        if (diff > TIMEOUT)
                        {
                            Thread t = new Thread(() => PerformTimeoutAlert(entry.Value));
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
            //horrible hack
            if (ap.GetType() == typeof(DifundPublishEventProperties))
                this.MainEntity.ActionTimedout((DifundPublishEventProperties) ap);

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
