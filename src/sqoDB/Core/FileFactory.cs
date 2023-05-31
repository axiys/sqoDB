﻿namespace sqoDB.Core
{
    internal class FileFactory
    {
        private static readonly object _syncRoot = new object();

        public static ISqoFile Create(string filePath, bool readOnly, bool elevatedTrust)
        {
            lock (_syncRoot)
            {
#if SILVERLIGHT
            if (elevatedTrust)
            {
                return new SqoFile(filePath, readOnly);
            }
            else if (SiaqodbConfigurator.UseLargeBuffers)
            {
                return new SqoISFile(filePath, readOnly);
            }
            else
            {
                return new SqoISFile2(filePath, readOnly);
            }
#elif CF
                return new SqoFile(filePath, readOnly);
#elif MONODROID
				return new SqoFile(filePath, readOnly);
#elif WinRT
                if (SiaqodbConfigurator.UseLargeBuffers)
                {
                    return new SqoWinRTMemoryFile(filePath, readOnly);
                }
                else
                {
                    return new SqoWinRTFile(filePath, readOnly);
                }
#else
                return new SqoFile(filePath, readOnly);

#endif
            }
        }
    }
}