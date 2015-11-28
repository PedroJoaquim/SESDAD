using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shared_Library
{
    public abstract class Command
    {
        public abstract void Execute(RemoteEntity entity);
    }

    public class ForwardEventCommand : Command
    {

        private Event e;
        private string targetSite;
        private int outSeqNumber;
        private int timeoutID;

        public ForwardEventCommand(Event e, string targetSite, int outSeqNumber, int timeoutID)
        {
            this.e = e;
            this.targetSite = targetSite;
            this.outSeqNumber = outSeqNumber;
            this.timeoutID = timeoutID;
        }

        public ForwardEventCommand(Event e, string targetSite, int outSeqNumber)
        {
            this.e = e;
            this.targetSite = targetSite;
            this.outSeqNumber = outSeqNumber;
            this.timeoutID = -1;
        }


        public override void Execute(RemoteEntity entity)
        {
            FaultManager fManager = entity.GetFaultManager();
            TimeoutMonitor tMonitor = fManager.TMonitor;
            try
            {
                IRemoteBroker broker = fManager.ChooseBroker(this.targetSite, this.e.Publisher);
                int newTimeoutID;

                if (this.timeoutID == -1)
                    newTimeoutID = tMonitor.NewActionPerformed(e, outSeqNumber, targetSite);
                else
                    newTimeoutID = tMonitor.NewActionPerformed(e, outSeqNumber, targetSite, timeoutID);


                Console.WriteLine(String.Format("[FORWARD EVENT] Topic: {0} Publisher: {1} EventNr: {2} To: {3}", e.Topic, e.Publisher, e.EventNr, entity.RemoteNetwork.GetBrokerName(broker)));

                broker.DifundPublishEvent(e, entity.RemoteNetwork.SiteName, entity.Name, outSeqNumber, newTimeoutID);
            } catch (Exception)
            {
                //ignore 
            }
        }
    }
}
