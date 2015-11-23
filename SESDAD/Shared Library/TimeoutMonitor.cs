using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shared_Library
{

    public class TimeoutMonitor
    {
        private const int SLEEP_TIME = 3000; //miliseconds
        private const int TIMEOUT = 1000; //miliseconds

        private RemoteEntity mainEntity;
        private int actionsID;

        //actions that the mainEntity performed and is waiting confirmation
        private Dictionary<int, ActionProperties> performedActions = new Dictionary<int, ActionProperties>();

        public TimeoutMonitor(RemoteEntity mainEntity)
        {
            this.mainEntity = mainEntity;
            this.actionsID = 1;
            Thread t = new Thread(MonitorizeTimeOuts);
            t.Start();
        }

        public int NewActionPerformed(Event e, int outSeqNumber, string targetEntity)
        {
            int newActionId = IncActionID();

            lock(this)
            {
                this.performedActions.Add(newActionId, new DifundPublishEventProperties(newActionId, targetEntity, e, outSeqNumber));
            }

            return newActionId;
        }

        public int NewActionPerformed(string targetEntity, string topic, bool unsubscribe)
        {
            int newActionId = IncActionID();

            lock (this)
            {
                this.performedActions.Add(newActionId, new DifundSubscribeEventProperties(newActionId, targetEntity, topic, unsubscribe));
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
                this.mainEntity.ActionTimedout((DifundPublishEventProperties) ap);
            else
                this.mainEntity.ActionTimedout((DifundSubscribeEventProperties) ap);
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
        private String targetEntity;
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
        #endregion

        public ActionProperties(int id, String targetEntity)
        {
            this.id = id;
            this.creationTime = DateTime.Now;
            this.targetEntity = targetEntity;
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

        public DifundPublishEventProperties(int id, string targetEntity, Event e, int outSeqNumber) : base(id, targetEntity)
        {
            this.E = e;
            this.OutSeqNumber = outSeqNumber;
        }
    }

    public class DifundSubscribeEventProperties : ActionProperties
    {
        private bool unsubscribe;
        private string topic;

        #region "Properties"
        public bool Unsubscribe
        {
            get
            {
                return unsubscribe;
            }

            set
            {
                unsubscribe = value;
            }
        }

        public string Topic
        {
            get
            {
                return topic;
            }

            set
            {
                topic = value;
            }
        }
        #endregion

        public DifundSubscribeEventProperties(int id, string targetEntity, string topic, bool unsubscribe) : base(id, targetEntity)
        {
            this.Topic = topic;
            this.Unsubscribe = unsubscribe;
        }
    }
}
