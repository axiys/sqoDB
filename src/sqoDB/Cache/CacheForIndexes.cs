using System.Collections.Generic;
using sqoDB.Indexes;
using sqoDB.Meta;
using sqoDB.Utilities;

namespace sqoDB.Cache
{
    internal class CacheForIndexes
    {
        private readonly Dictionary<SqoTypeInfo, Dictionary<FieldSqoInfo, IBTree>> cache =
            new Dictionary<SqoTypeInfo, Dictionary<FieldSqoInfo, IBTree>>();

        public void Add(SqoTypeInfo ti, Dictionary<FieldSqoInfo, IBTree> dictionary)
        {
            cache[ti] = dictionary;
        }

        public IBTree GetIndex(SqoTypeInfo type, FieldSqoInfo fi)
        {
            if (cache.ContainsKey(type))
                if (cache[type].ContainsKey(fi))
                    return cache[type][fi];
            return null;
        }

        public IBTree GetIndex(SqoTypeInfo type, string fieldName)
        {
            if (cache.ContainsKey(type))
            {
                var fi = MetaHelper.FindField(type.Fields, fieldName);
                if (fi != null)
                    return GetIndex(type, fi);
            }

            return null;
        }

        public bool ContainsType(SqoTypeInfo type)
        {
            return cache.ContainsKey(type);
        }

        public bool RemoveType(SqoTypeInfo type)
        {
            if (cache.ContainsKey(type)) return cache.Remove(type);
            return false;
        }

        public Dictionary<FieldSqoInfo, IBTree> GetIndexes(SqoTypeInfo ti)
        {
            if (cache.ContainsKey(ti)) return cache[ti];
            return null;
        }

        public void Set(SqoTypeInfo ti, FieldSqoInfo fi, IBTree index)
        {
            cache[ti][fi] = index;
        }
    }
}