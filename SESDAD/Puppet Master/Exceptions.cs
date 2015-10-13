using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PuppetMaster
{
    public class ConfigFileParseException : Exception
    {
        public ConfigFileParseException() { }

        public ConfigFileParseException(String message) : base(message) { }

        public ConfigFileParseException(String message, Exception inner) : base(message, inner) { }

    }
}
