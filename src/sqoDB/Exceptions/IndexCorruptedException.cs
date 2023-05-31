using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sqoDB.Exceptions
{
    public class IndexCorruptedException : Exception
    {
        public IndexCorruptedException()
            : base()
        {

        }
        public IndexCorruptedException(string message)
            : base(message)
        {

        }
    }
}
