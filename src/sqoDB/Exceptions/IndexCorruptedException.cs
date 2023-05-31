using System;

namespace sqoDB.Exceptions
{
    public class IndexCorruptedException : Exception
    {
        public IndexCorruptedException()
        {
        }

        public IndexCorruptedException(string message)
            : base(message)
        {
        }
    }
}