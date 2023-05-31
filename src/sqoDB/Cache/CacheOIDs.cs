using System.Collections.Generic;
using System.Reflection;
using sqoDB.Meta;

namespace sqoDB.Cache
{
    internal class CacheOIDs
    {
        private readonly Dictionary<SqoTypeInfo, ConditionalWeakTable> dict =
            new Dictionary<SqoTypeInfo, ConditionalWeakTable>();

        public void AddTypeInfo(SqoTypeInfo ti)
        {
            dict[ti] = new ConditionalWeakTable();
        }

        private void AddObjectOID(SqoTypeInfo ti, object obj, int oid)
        {
            if (dict.ContainsKey(ti))
            {
                int oidAlreadyRegistered;
                if (dict[ti].TryGetValue(obj, out oidAlreadyRegistered))
                {
                    //do nothing, let existing object there
                }
                else
                {
                    dict[ti].Add(obj, oid);
                }
            }
        }

        public void SetOIDToObject(object obj, int oid, SqoTypeInfo ti)
        {
            var dataObje = obj as ISqoDataObject;
            if (dataObje != null)
            {
                dataObje.OID = oid;
            }
            else //set oid by reflection
            {
                var flags = BindingFlags.Instance | BindingFlags.Public;

                var pi = ti.Type.GetProperty("OID", flags);
                if (pi == null)
                {
                    //throw new SiaqodbException("Object of Type:" + ti.ToString() + " does not have property OID, define it first!");
                    AddObjectOID(ti, obj, oid);
                }
                else
                {
#if UNITY3D
                     pi.GetSetMethod().Invoke(obj, new object[]{oid});

#else
                    pi.SetValue(obj, oid, null);
#endif
                }
            }
        }

        public int GetOIDOfObject(object obj, SqoTypeInfo ti)
        {
            var dataObje = obj as ISqoDataObject;
            if (dataObje != null) return dataObje.OID;

            //get oid by reflection
            var flags = BindingFlags.Instance | BindingFlags.Public;

            var pi = ti.Type.GetProperty("OID", flags);
            if (pi == null)
                //throw new SiaqodbException("Object of Type:" + ti.ToString() + " does not have property OID, define it first!");
                return GetOID(ti, obj);
#if UNITY3D
                    return (int)pi.GetGetMethod().Invoke(obj, null);
#else

            return (int)pi.GetValue(obj, null);
#endif
        }

        private int GetOID(SqoTypeInfo ti, object obj)
        {
            if (dict.ContainsKey(ti))
            {
                int oid;
                var found = dict[ti].TryGetValue(obj, out oid);
                if (found)
                    return oid;
            }

            return 0;
        }
    }
}