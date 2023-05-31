using System.Collections.Generic;
using sqoDB.Meta;

namespace sqoDB.Cache
{
    internal class CacheDocuments
    {
        private readonly Dictionary<SqoTypeInfo, ConditionalWeakTable> dict =
            new Dictionary<SqoTypeInfo, ConditionalWeakTable>();

        public void AddDocument(SqoTypeInfo ti, object parentObjOfDocument, string fieldName, int docinfoOid)
        {
            if (!dict.ContainsKey(ti)) dict.Add(ti, new ConditionalWeakTable());
            dict[ti].Add(new DocumentCacheObject(parentObjOfDocument, fieldName), docinfoOid);
        }

        public int GetDocumentInfoOID(SqoTypeInfo ti, object parentObjOfDocument, string fieldName)
        {
            if (dict.ContainsKey(ti))
            {
                int oid;
                var found = dict[ti].TryGetValue(new DocumentCacheObject(parentObjOfDocument, fieldName), out oid);
                if (found)
                    return oid;
            }

            return 0;
        }
    }

    internal class DocumentCacheObject
    {
        public DocumentCacheObject(object parent, string fieldName)
        {
            Parent = parent;
            FieldName = fieldName;
        }

        public object Parent { get; set; }
        public string FieldName { get; set; }

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 17;
                hash = hash * 31 + Parent.GetHashCode();
                hash = hash * 31 + FieldName.GetHashCode();
                return hash;
            }
        }

        public override bool Equals(object obj)
        {
            var doc = obj as DocumentCacheObject;
            return Parent == doc.Parent && FieldName == doc.FieldName;
        }
    }
}