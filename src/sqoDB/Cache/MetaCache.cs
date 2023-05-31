using System;
using System.Collections.Generic;
using System.Linq;
using sqoDB.Meta;
using sqoDB.Utilities;

namespace sqoDB.Cache
{
    internal class MetaCache
    {
        private readonly CacheDocuments cacheDocs;
        private readonly Dictionary<Type, SqoTypeInfo> cacheOfTypes = new Dictionary<Type, SqoTypeInfo>();
        private readonly CacheOIDs cacheOIDs;
        private int nextTID;

        public MetaCache()
        {
            cacheOIDs = new CacheOIDs();
            cacheDocs = new CacheDocuments();
        }

        public void AddType(Type type, SqoTypeInfo ti)
        {
            cacheOfTypes[type] = ti;
            SetMaxTID(ti.Header.TID);
            if (!MetaHelper.TypeHasOID(type))
                cacheOIDs.AddTypeInfo(ti);
        }

        public bool Contains(Type type)
        {
            return cacheOfTypes.ContainsKey(type);
        }

        public void Remove(Type type)
        {
            if (cacheOfTypes.ContainsKey(type)) cacheOfTypes.Remove(type);
        }

        public SqoTypeInfo GetSqoTypeInfo(Type t)
        {
            return cacheOfTypes[t];
        }

        public List<SqoTypeInfo> DumpAllTypes()
        {
            var types = new List<SqoTypeInfo>();
            foreach (var ti in cacheOfTypes.Values) types.Add(ti);
            return types;
        }

        public SqoTypeInfo GetSqoTypeInfoByTID(int tid)
        {
            return cacheOfTypes.Values.First(tii => tii.Header.TID == tid);
        }

        public int GetNextTID()
        {
            ++nextTID;

            return nextTID;
        }

        public void SetMaxTID(int tid)
        {
            if (nextTID < tid) nextTID = tid;
        }

        public void SetOIDToObject(object obj, int oid, SqoTypeInfo ti)
        {
            cacheOIDs.SetOIDToObject(obj, oid, ti);
        }

        public int GetOIDOfObject(object obj, SqoTypeInfo ti)
        {
            return cacheOIDs.GetOIDOfObject(obj, ti);
        }

        public void AddDocument(SqoTypeInfo ti, object parentObjOfDocument, string fieldName, int docinfoOid)
        {
            cacheDocs.AddDocument(ti, parentObjOfDocument, fieldName, docinfoOid);
        }

        public int GetDocumentInfoOID(SqoTypeInfo ti, object parentObjOfDocument, string fieldName)
        {
            return cacheDocs.GetDocumentInfoOID(ti, parentObjOfDocument, fieldName);
        }
    }
}