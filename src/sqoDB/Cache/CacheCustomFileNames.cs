﻿using System.Collections.Generic;
using sqoDB.Exceptions;

namespace sqoDB.Cache
{
    internal class CacheCustomFileNames
    {
        private static readonly Dictionary<string, string> customFiles = new Dictionary<string, string>();

        public static void AddFileNameForType(string typeName, string fileName)
        {
            AddFileNameForType(typeName, fileName, true);
        }

        public static void AddFileNameForType(string typeName, string fileName, bool throwExceptionIfDuplicate)
        {
            if (throwExceptionIfDuplicate)
                if (customFiles.ContainsKey(typeName))
                    throw new SiaqodbException("This Type:" + typeName + " already has set customFileName");
            foreach (var key in customFiles.Keys)
            {
                if (key == typeName) continue;
                if (customFiles[key] == fileName)
                    throw new SiaqodbException("This customFileName is set for another Type:" + customFiles[key]);
            }

            customFiles[typeName] = fileName;
        }

        public static void RemoveFileNameForType(string typeName)
        {
            if (customFiles.ContainsKey(typeName)) customFiles.Remove(typeName);
        }

        public static string GetFileName(string typeName)
        {
            if (customFiles.ContainsKey(typeName)) return customFiles[typeName];
            return null;
        }
    }
}