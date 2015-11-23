using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shared_Library;

namespace Broker
{
    class FaultManager
    {
        private RemoteEntity mainEntity;
        private TimeoutMonitor timeoutMonitor;
        public FaultManager(RemoteEntity re)
        {
            this.mainEntity = re;
            this.timeoutMonitor = re.TMonitor;
        }

        // tuple = targetEntity , outSeqNumber
        public void FMPublishEvent(Event e, string targetEntity, int outSeqNumber)
        {

        }



    }
}
