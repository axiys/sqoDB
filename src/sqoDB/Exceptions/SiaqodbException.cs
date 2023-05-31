using System;

namespace sqoDB.Exceptions
{
    public class SiaqodbException : Exception
    {
        public SiaqodbException()
        {
        }

        public SiaqodbException(string message) : base(message)
        {
        }
    }
}