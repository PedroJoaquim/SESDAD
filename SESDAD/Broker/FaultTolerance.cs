using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;
using System.Threading;

namespace Broker
{
    class FaultManager : ITimeoutListener
    {
        private int actionID;
        private RemoteEntity mainEntity;
        private TimeoutMonitor timeoutMonitor;
        private Dictionary<int, Dictionary<int, bool>> waitingEvents;
        private Dictionary<int, Pair<int, bool>> waitingRooms;
        private Dictionary<int, int> timeoutActionMap;
    
        public FaultManager(RemoteEntity re)
        {
            actionID = 1;
            this.mainEntity = re;
            this.timeoutMonitor = re.TMonitor;
            this.timeoutMonitor.MainEntity = this; //different for brokers
            waitingEvents = new Dictionary<int, Dictionary<int, bool>>();
            waitingRooms = new Dictionary<int, Pair<int, bool>>();
            timeoutActionMap = new Dictionary<int, int>();
        }

        /*
         *  Method that returns a new actionID - threadsafe
         */
        public int NextActionID()
        {
            lock(this)
            {
                int newID = actionID;
                this.actionID++;
                return newID;
            }
        }

        /*
         * First call to every broker, we have to send all before we begin accpeting acks
         */

        public int FMMultiplePublishEvent(Event e, List<Tuple<string, int>> targetSites)
        {
            int timeoutID;
            int actionID = NextActionID();

            lock(this)
            {
                waitingEvents[actionID] = new Dictionary<int, bool>();
                waitingRooms[actionID] = new Pair<int, bool>(actionID, false);
            }


            lock (waitingRooms[actionID])//do not allow acks before we send them all
            {
                foreach (Tuple<string, int> entry in targetSites)
                {
                    string site = entry.Item1;
                    int outSeqNumber = entry.Item2;

                    timeoutID = timeoutMonitor.NewActionPerformed(e, outSeqNumber, site);

                    lock(this)
                    {
                        waitingEvents[actionID][timeoutID] = false;
                        timeoutActionMap[timeoutID] = actionID;
                    }


                    new Task(() => { ExecuteEventTransmission(e, site, outSeqNumber, timeoutID, false); }).Start();
                }
            }

            return actionID;
        }

        /*
         * Restransmission event
         */
        private void FMPublishEventRetransmission(Event e, string targetSite, int outSeqNumber, int actionID, int oldTimeoutID)
        {
            lock (waitingRooms[actionID])
            {

                lock (this)
                {
                    if (waitingEvents[actionID][oldTimeoutID])
                        return; //acked already received, possible desync

                    waitingEvents[actionID].Remove(oldTimeoutID);
                    timeoutActionMap.Remove(oldTimeoutID);
                }

                int newTimeoutID = timeoutMonitor.NewActionPerformed(e, outSeqNumber, targetSite);

                lock(this)
                {
                    waitingEvents[actionID][newTimeoutID] = false;
                    timeoutActionMap[newTimeoutID] = actionID;
                }

                new Task(() => { ExecuteEventTransmission(e, targetSite, outSeqNumber, newTimeoutID, true); }).Start();
            }
        }

        private void ExecuteEventTransmission(Event e, string targetSite, int outSeqNumber, int timeoutID, bool retransmission)
        {
            mainEntity.RemoteNetwork.ChooseBroker(targetSite, e.Publisher, retransmission).DifundPublishEvent(e, mainEntity.RemoteNetwork.SiteName, mainEntity.Name, outSeqNumber, timeoutID);
        }

        /*
         * Wait untill all publish events are acked
         */ 

        public void WaitEventDistribution(int actionID)
        {
            Pair<int, bool> waiting = waitingRooms[actionID];

            lock(waiting)
            {
                while (!waiting.Second) 
                {
                    Monitor.Wait(waiting);
                }

                RemoveElements(actionID); //remove elements associated with actionID

            }
        }

        private void RemoveElements(int actionID)
        {
            lock(this)
            {
                waitingRooms.Remove(actionID);
                waitingEvents.Remove(actionID);

                List<int> toBeRemoved = new List<int>();

                foreach (KeyValuePair<int, int> item in timeoutActionMap)
                {
                    if (item.Value == actionID)
                        toBeRemoved.Add(item.Key);
                }

                foreach(int key in toBeRemoved)
                {
                    timeoutActionMap.Remove(key);
                }
            }
        }

        /*
         * ITimeoutListener Interface Implementation
         */

        public void ActionTimedout(DifundPublishEventProperties p)
        {
            int actionID = timeoutActionMap[p.Id];

            FMPublishEventRetransmission(p.E, p.TargetSite, p.OutSeqNumber, actionID, p.Id);
        }


        public void ActionACKReceived(int timeoutID)
        {
        
            int actionID = timeoutActionMap[timeoutID];

            Pair <int, bool> waiting = waitingRooms[actionID];

            lock (waiting)
            {
                waitingEvents[actionID][timeoutID] = true;
                bool allAcksReceived = true;

                foreach (KeyValuePair<int, bool> entry in waitingEvents[actionID])
                {
                    if (!entry.Value)
                    {
                        allAcksReceived = false;
                        break;
                    }
                }

                if(allAcksReceived)
                {
                    waiting.Second = true;
                    Monitor.PulseAll(waiting);
                }
            }
        }
    }
}
