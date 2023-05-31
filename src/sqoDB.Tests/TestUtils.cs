using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqoDBDB.Tests
{
    internal class TestUtils
    {
        private static string _objPath = null;

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
