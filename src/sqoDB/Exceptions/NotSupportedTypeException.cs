using System;

namespace sqoDB.Exceptions
{
    public class NotSupportedTypeException : Exception
    {
        public NotSupportedTypeException()
        {
        }

        public NotSupportedTypeException(string message) : base(message)
        {
        }
    }
}