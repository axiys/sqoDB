using System;

namespace sqoDB.Exceptions
{
    public class NotAsyncException : Exception
    {
        public NotAsyncException()
        {
        }

        public NotAsyncException(string message)
            : base(message)
        {
        }
    }
}