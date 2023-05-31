using System.Collections.Generic;

namespace sqoDB.Meta
{
    internal class ObjectInfo
    {
        public ObjectInfo(SqoTypeInfo ti, object backendObj)
        {
            SqoTypeInfo = ti;
            BackendObject = backendObj;
        }

        public SqoTypeInfo SqoTypeInfo { get; }

        public Dictionary<FieldSqoInfo, object> AtInfo { get; } = new Dictionary<FieldSqoInfo, object>();

        public int Oid { get; set; }

        public bool Inserted { get; set; }

        public ulong TickCount { get; set; }

        public object BackendObject { get; set; }
    }
}