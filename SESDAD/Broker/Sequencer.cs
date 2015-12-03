using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Broker
{

   
    class Sequencer
    {
        public const string SEQUENCER_BASE_NAME = "#sequencer#";

        private int nextSeqNumber;

        public Sequencer()
        {
            this.nextSeqNumber = 1;
        }

        public int GetNextSeqNumber()
        {
            lock(this)
            {
                int result = nextSeqNumber;
                this.nextSeqNumber++;
                return result;
            }
        }

    }
}
