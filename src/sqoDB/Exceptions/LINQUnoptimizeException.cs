using System;

namespace sqoDB.Exceptions
{
    public class LINQUnoptimizeException : Exception
    {
        public LINQUnoptimizeException()
        {
        }

        public LINQUnoptimizeException(string message)
            : base(message)
        {
        }
    }
}