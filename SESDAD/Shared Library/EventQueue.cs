using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shared_Library
{
    public class EventQueue
    {
        
        private Command[] buffer;
        private int size;
        private int busy;
        private int insCur;
        private int remCur;

        public EventQueue(int size)
        {
            buffer = new Command[size];
            this.size = size;
            busy = 0;
            insCur = 0;
            remCur = 0;
        }

    
        public void Produce(Command o)
        {
            lock (this)
            {
                while (busy == size)
                {
                    Monitor.Wait(this);
                }
                buffer[insCur] = o;
                insCur = ++insCur % size;
                busy++;
                if (busy == 1)
                {
                    Monitor.Pulse(this);
                }
            }
        }

        public Command Consume()
        {
            Command o;
            lock (this)
            {
                while (busy == 0)
                {
                    Monitor.Wait(this);
                }
                o = buffer[remCur];
                buffer[remCur] = default(Command);
                remCur = ++remCur % size;
                busy--;
                if (busy == size - 1)
                {
                    Monitor.Pulse(this);
                }
            }
            return o;
        }
    }
}
