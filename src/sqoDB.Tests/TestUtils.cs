using System;
using System.IO;

namespace sqoDBDB.Tests
{
    internal class TestUtils
    {
        private static string _objPath;

        public static string GetTempPath()
        {
            if (string.IsNullOrEmpty(_objPath))
            {
                var newPath = Path.GetTempPath() + @"\" + Guid.NewGuid();
                Directory.CreateDirectory(newPath);
                _objPath = newPath;
            }

            return _objPath;
        }
    }
}