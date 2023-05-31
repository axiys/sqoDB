using System.Collections.Generic;
using sqoDB.Meta;

namespace sqoDB.Cache
{
    internal class CircularRefCache
    {
        private readonly List<CircularRefChacheItem> list = new List<CircularRefChacheItem>();

        public void Add(int oid, SqoTypeInfo ti, object obj)
        {
            var item = new CircularRefChacheItem { OID = oid, TInfo = ti, Obj = obj };
            list.Add(item);
        }

        public void Clear()
        {
            list.Clear();
        }

        public object GetObject(int oid, SqoTypeInfo ti)
        {
            foreach (var item in list)
                if (item.OID == oid && item.TInfo == ti)
                    return item.Obj;
            return null;
        }

        private class CircularRefChacheItem
        {
            public int OID { get; set; }
            public SqoTypeInfo TInfo { get; set; }
            public object Obj { get; set; }
        }
    }
}