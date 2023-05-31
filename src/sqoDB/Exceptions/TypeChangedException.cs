using System;

namespace sqoDB.Exceptions
{
    public class TypeChangedException : Exception
    {
        public TypeChangedException()
        {
        }

        public TypeChangedException(string message)
            : base(message)
        {
        }
    }
}