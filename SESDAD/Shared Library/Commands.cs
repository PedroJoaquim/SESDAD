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
        private IRemoteBroker targetBroker;

        public ForwardEventCommand(Event e, string targetSite, int outSeqNumber, int timeoutID, IRemoteBroker targetBroker)
        {
            this.e = e;
            this.targetSite = targetSite;
            this.outSeqNumber = outSeqNumber;
            this.timeoutID = timeoutID;
            this.targetBroker = targetBroker;
        }


        public override void Execute(RemoteEntity entity)
        {
            try
            {
                targetBroker.DifundPublishEvent(e, entity.RemoteNetwork.SiteName, entity.Name, outSeqNumber, timeoutID);
            } catch (Exception)
            {
                //ignore 
            }
        }
    }
}
