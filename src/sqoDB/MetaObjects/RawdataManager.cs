using sqoDB.Meta;
using sqoDB.Queries;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.MetaObjects
{
    internal class RawdataManager
    {
        private readonly StorageEngine storageEngine;

        public RawdataManager(StorageEngine storageEngine)
        {
            this.storageEngine = storageEngine;
        }

        public RawdataInfo GetRawdataInfo(int oid)
        {
            var info = storageEngine.LoadObjectByOID<RawdataInfo>(GetSqoTypeInfo(), oid, false);
            return info;
        }
#if ASYNC
        public async Task<RawdataInfo> GetRawdataInfoAsync(int oid)
        {
            var info = await storageEngine.LoadObjectByOIDAsync<RawdataInfo>(GetSqoTypeInfo(), oid, false)
                .ConfigureAwait(false);
            return info;
        }
#endif

        public RawdataInfo GetFreeRawdataInfo(int rawLength)
        {
            var w = new Where("IsFree", OperationType.Equal, true);
            w.StorageEngine = storageEngine;
            w.ParentSqoTypeInfo = GetSqoTypeInfo();
            w.ParentType.Add(w.ParentSqoTypeInfo.Type);
            var w1 = new Where("Length", OperationType.GreaterThanOrEqual, rawLength);
            w1.StorageEngine = storageEngine;
            w1.ParentSqoTypeInfo = GetSqoTypeInfo();
            w1.ParentType.Add(w1.ParentSqoTypeInfo.Type);
            var and = new And();
            and.Add(w, w1);

            var oids = and.GetOIDs();
            if (oids.Count > 0) return GetRawdataInfo(oids[0]);

            return null;
        }
#if ASYNC
        public async Task<RawdataInfo> GetFreeRawdataInfoAsync(int rawLength)
        {
            var w = new Where("IsFree", OperationType.Equal, true);
            w.StorageEngine = storageEngine;
            w.ParentSqoTypeInfo = GetSqoTypeInfo();
            w.ParentType.Add(w.ParentSqoTypeInfo.Type);
            var w1 = new Where("Length", OperationType.GreaterThanOrEqual, rawLength);
            w1.StorageEngine = storageEngine;
            w1.ParentSqoTypeInfo = GetSqoTypeInfo();
            w1.ParentType.Add(w1.ParentSqoTypeInfo.Type);
            var and = new And();
            and.Add(w, w1);

            var oids = await and.GetOIDsAsync().ConfigureAwait(false);
            if (oids.Count > 0) return await GetRawdataInfoAsync(oids[0]).ConfigureAwait(false);

            return null;
        }
#endif
        public void SaveRawdataInfo(RawdataInfo rawdataInfo)
        {
            storageEngine.SaveObject(rawdataInfo, GetSqoTypeInfo());
        }
#if ASYNC
        public async Task SaveRawdataInfoAsync(RawdataInfo rawdataInfo)
        {
            await storageEngine.SaveObjectAsync(rawdataInfo, GetSqoTypeInfo()).ConfigureAwait(false);
        }
#endif
        public int GetNextOID()
        {
            var ti = GetSqoTypeInfo();
            return ti.Header.numberOfRecords + 1;
        }

        private SqoTypeInfo GetSqoTypeInfo()
        {
            SqoTypeInfo ti = null;
            if (storageEngine.metaCache.Contains(typeof(RawdataInfo)))
            {
                ti = storageEngine.metaCache.GetSqoTypeInfo(typeof(RawdataInfo));
            }
            else
            {
                ti = MetaExtractor.GetSqoTypeInfo(typeof(RawdataInfo));

                storageEngine.SaveType(ti);
                storageEngine.metaCache.AddType(typeof(RawdataInfo), ti);
            }

            return ti;
        }

        internal void MarkRawInfoAsFree(int oid)
        {
            storageEngine.SaveValue(oid, "IsFree", GetSqoTypeInfo(), true);
        }
#if ASYNC
        internal async Task MarkRawInfoAsFreeAsync(int oid)
        {
            await storageEngine.SaveValueAsync(oid, "IsFree", GetSqoTypeInfo(), true).ConfigureAwait(false);
        }
#endif
    }
}