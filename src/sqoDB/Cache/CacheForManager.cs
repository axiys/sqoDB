using System.Collections.Generic;
using sqoDB.Meta;

namespace sqoDB.Cache
{
    internal class CacheForManager
    {
        private readonly Dictionary<string, SqoTypeInfo> cache = new Dictionary<string, SqoTypeInfo>();

        public void AddType(string type, SqoTypeInfo ti)
        {
            cache[type] = ti;
        }

        public SqoTypeInfo GetSqoTypeInfo(string t)
        {
            return cache[t];
        }

        public bool Contains(string type)
        {
            return cache.ContainsKey(type);
        }
    }
}