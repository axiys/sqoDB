using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using sqoDB.Core;
using sqoDB.Exceptions;
using sqoDB.Meta;
using sqoDB.Queries;
using sqoDB.Transactions;
using sqoDB.Utilities;
#if ASYNC
using System.Threading.Tasks;
#endif

#if SILVERLIGHT
	using System.IO.IsolatedStorage;
#endif
namespace sqoDB
{
    internal partial class StorageEngine
    {
        internal void SaveObject(object oi, SqoTypeInfo ti, ObjectInfo objInfo, Transaction transaction)
        {
            var trObject = new TransactionObject(this);
            trObject.currentObject = oi;
            trObject.objInfo = objInfo == null ? MetaExtractor.GetObjectInfo(oi, ti, metaCache) : objInfo;
            trObject.serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            trObject.Operation = TransactionObject.OperationType.InsertOrUpdate;

            CheckForConcurencyOnly(oi, trObject.objInfo, ti, trObject.serializer);

            CheckConstraints(trObject.objInfo, ti);

            TransactionManager.transactions[transaction.ID].AddTransactionObject(trObject);
        }
#if ASYNC
        internal async Task SaveObjectAsync(object oi, SqoTypeInfo ti, ObjectInfo objInfo, Transaction transaction)
        {
            var trObject = new TransactionObject(this);
            trObject.currentObject = oi;
            trObject.objInfo = objInfo == null ? MetaExtractor.GetObjectInfo(oi, ti, metaCache) : objInfo;
            trObject.serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            trObject.Operation = TransactionObject.OperationType.InsertOrUpdate;

            await CheckForConcurencyOnlyAsync(oi, trObject.objInfo, ti, trObject.serializer).ConfigureAwait(false);

            await CheckConstraintsAsync(trObject.objInfo, ti).ConfigureAwait(false);

            TransactionManager.transactions[transaction.ID].AddTransactionObject(trObject);
        }
#endif
        internal int SaveObject(object oi, SqoTypeInfo ti)
        {
            var objInfo = MetaExtractor.GetObjectInfo(oi, ti, metaCache);

            return SaveObject(oi, ti, objInfo);
        }
#if ASYNC
        internal async Task<int> SaveObjectAsync(object oi, SqoTypeInfo ti)
        {
            var objInfo = MetaExtractor.GetObjectInfo(oi, ti, metaCache);

            return await SaveObjectAsync(oi, ti, objInfo).ConfigureAwait(false);
        }
#endif
        internal int SaveObject(object oi, SqoTypeInfo ti, ObjectInfo objInfo)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedSaveComplexObject += serializer_NeedSaveComplexObject;
            CheckForConcurency(oi, objInfo, ti, serializer, false);

            CheckConstraints(objInfo, ti);

            var oldValuesOfIndexedFields = indexManager.PrepareUpdateIndexes(objInfo, ti);

            serializer.SerializeObject(objInfo, rawSerializer);

            metaCache.SetOIDToObject(oi, objInfo.Oid, ti);

            indexManager.UpdateIndexes(objInfo, ti, oldValuesOfIndexedFields);

            return objInfo.Oid;
        }
#if ASYNC
        internal async Task<int> SaveObjectAsync(object oi, SqoTypeInfo ti, ObjectInfo objInfo)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedSaveComplexObjectAsync += serializer_NeedSaveComplexObjectAsync;
            await CheckForConcurencyAsync(oi, objInfo, ti, serializer, false).ConfigureAwait(false);

            await CheckConstraintsAsync(objInfo, ti).ConfigureAwait(false);

            var oldValuesOfIndexedFields =
                await indexManager.PrepareUpdateIndexesAsync(objInfo, ti).ConfigureAwait(false);

            await serializer.SerializeObjectAsync(objInfo, rawSerializer).ConfigureAwait(false);

            metaCache.SetOIDToObject(oi, objInfo.Oid, ti);

            await indexManager.UpdateIndexesAsync(objInfo, ti, oldValuesOfIndexedFields).ConfigureAwait(false);

            return objInfo.Oid;
        }
#endif
        internal void SaveObjectPartially(object obj, SqoTypeInfo ti, string[] properties)
        {
            foreach (var path in properties)
            {
                var arrayPath = path.Split('.');

                PropertyInfo property;
                var type = ti.Type;
                var objOfProp = obj;
                var tiOfProp = ti;
                var oid = -1;
                string backingField = null;
                foreach (var include in arrayPath)
                {
                    if ((property = type.GetProperty(include)) == null)
                        throw new SiaqodbException("Property:" + include + " does not belong to Type:" + type.FullName);
                    backingField = ExternalMetaHelper.GetBackingField(property);

                    tiOfProp = GetSqoTypeInfoSoft(type);

                    var val = MetaExtractor.GetPartialObjectInfo(objOfProp, tiOfProp, backingField, metaCache);
                    objOfProp = val.Value;
                    oid = val.Name;
                    if (oid == 0) throw new SiaqodbException("Only updates are allowed through this method.");
                    type = property.PropertyType;
                }

                var oldPropVal = indexManager.GetValueForFutureUpdateIndex(oid, backingField, tiOfProp);
                SaveValue(oid, backingField, tiOfProp, objOfProp);
                indexManager.UpdateIndexes(oid, backingField, tiOfProp, oldPropVal, objOfProp);
            }
        }
#if ASYNC
        internal async Task SaveObjectPartiallyAsync(object obj, SqoTypeInfo ti, string[] properties)
        {
            foreach (var path in properties)
            {
                var arrayPath = path.Split('.');

                PropertyInfo property;
                var type = ti.Type;
                var objOfProp = obj;
                var tiOfProp = ti;
                var oid = -1;
                string backingField = null;
                foreach (var include in arrayPath)
                {
                    if ((property = type.GetProperty(include)) == null)
                        throw new SiaqodbException("Property:" + include + " does not belong to Type:" + type.FullName);
                    backingField = ExternalMetaHelper.GetBackingField(property);

                    tiOfProp = GetSqoTypeInfoSoft(type);

                    var val = MetaExtractor.GetPartialObjectInfo(objOfProp, tiOfProp, backingField, metaCache);
                    objOfProp = val.Value;
                    oid = val.Name;
                    if (oid == 0) throw new SiaqodbException("Only updates are allowed through this method.");
                    type = property.PropertyType;
                }

                var oldPropVal = await indexManager.GetValueForFutureUpdateIndexAsync(oid, backingField, tiOfProp);
                await SaveValueAsync(oid, backingField, tiOfProp, objOfProp).ConfigureAwait(false);
                await indexManager.UpdateIndexesAsync(oid, backingField, tiOfProp, oldPropVal, objOfProp);
            }
        }
#endif
        internal void SaveObjectPartiallyByFields(object obj, SqoTypeInfo ti, string[] fields)
        {
            foreach (var fieldName in fields)
            {
                var val = MetaExtractor.GetPartialObjectInfo(obj, ti, fieldName, metaCache);
                var objOfProp = val.Value;
                var oid = val.Name;
                if (oid == 0) throw new SiaqodbException("Only updates are allowed through this method.");
                SaveValue(oid, fieldName, ti, objOfProp);
            }
        }
#if ASYNC
        internal async Task SaveObjectPartiallyByFieldsAsync(object obj, SqoTypeInfo ti, string[] fields)
        {
            foreach (var fieldName in fields)
            {
                var val = MetaExtractor.GetPartialObjectInfo(obj, ti, fieldName, metaCache);
                var objOfProp = val.Value;
                var oid = val.Name;
                if (oid == 0) throw new SiaqodbException("Only updates are allowed through this method.");
                await SaveValueAsync(oid, fieldName, ti, objOfProp).ConfigureAwait(false);
            }
        }
#endif
        internal bool SaveValue(int oid, string field, SqoTypeInfo ti, object value)
        {
            if (field == "OID") throw new SiaqodbException("OID cannot be saved from client code!");
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedSaveComplexObject += serializer_NeedSaveComplexObject;

            if (oid > 0 && oid <= ti.Header.numberOfRecords && !serializer.IsObjectDeleted(oid, ti))
                return serializer.SaveFieldValue(oid, field, ti, value, rawSerializer);
            return false;
        }
#if ASYNC
        internal async Task<bool> SaveValueAsync(int oid, string field, SqoTypeInfo ti, object value)
        {
            if (field == "OID") throw new SiaqodbException("OID cannot be saved from client code!");
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedSaveComplexObjectAsync += serializer_NeedSaveComplexObjectAsync;

            if (oid > 0 && oid <= ti.Header.numberOfRecords &&
                !await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false))
                return await serializer.SaveFieldValueAsync(oid, field, ti, value, rawSerializer).ConfigureAwait(false);
            return false;
        }
#endif
        internal int InsertObjectByMeta(SqoTypeInfo tinf)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(tinf), useElevatedTrust);
            return serializer.InsertEmptyObject(tinf);
        }

        internal bool UpdateObjectBy(string[] fieldNames, object obj, SqoTypeInfo ti, Transaction transact)
        {
            var objInfo = MetaExtractor.GetObjectInfo(obj, ti, metaCache);
            var i = 0;
            ICriteria wPrev = null;

            foreach (var fieldName in fieldNames)
            {
                var fi = MetaHelper.FindField(ti.Fields, fieldName);
                if (fi == null)
                    throw new SiaqodbException("Field:" + fieldName + " was not found as member of Type:" +
                                               ti.TypeName);

                var w = new Where(fieldName, OperationType.Equal, objInfo.AtInfo[fi]);
                w.StorageEngine = this;
                w.ParentSqoTypeInfo = ti;
                w.ParentType.Add(w.ParentSqoTypeInfo.Type);

                if (i > 0)
                {
                    var and = new And();
                    and.Add(w, wPrev);

                    wPrev = and;
                }
                else
                {
                    wPrev = w;
                }

                i++;
            }

            var oids = wPrev.GetOIDs();
            if (oids.Count > 1)
                throw new SiaqodbException("In database exists more than one object with value of fields specified");

            if (oids.Count == 1)
            {
                objInfo.Oid = oids[0];

                #region old code that was duplicated like on SaveObject

                /*//obj.OID = oids[0];
                MetaHelper.SetOIDToObject(obj, oids[0], ti.Type);

                lock (_syncRoot)
                {
                    ObjectSerializer serializer = SerializerFactory.GetSerializer(this.path, GetFileByType(ti), useElevatedTrust);
                    
                    CheckForConcurency(obj, objInfo, ti, serializer,false);
                    
                    CheckConstraints(objInfo, ti);

                    Dictionary<string, object> oldValuesOfIndexedFields = this.PrepareUpdateIndexes(objInfo, ti, serializer);

                    serializer.SerializeObject(objInfo);

                    this.UpdateIndexes(objInfo, ti, oldValuesOfIndexedFields);
                }*/

                #endregion

                if (transact == null)
                    SaveObject(obj, ti, objInfo);
                else
                    SaveObject(obj, ti, objInfo, transact);
                return true;
            }

            return false;
        }
#if ASYNC
        internal async Task<bool> UpdateObjectByAsync(string[] fieldNames, object obj, SqoTypeInfo ti,
            Transaction transact)
        {
            var objInfo = MetaExtractor.GetObjectInfo(obj, ti, metaCache);
            var i = 0;
            ICriteria wPrev = null;

            foreach (var fieldName in fieldNames)
            {
                var fi = MetaHelper.FindField(ti.Fields, fieldName);
                if (fi == null)
                    throw new SiaqodbException("Field:" + fieldName + " was not found as member of Type:" +
                                               ti.TypeName);

                var w = new Where(fieldName, OperationType.Equal, objInfo.AtInfo[fi]);
                w.StorageEngine = this;
                w.ParentSqoTypeInfo = ti;
                w.ParentType.Add(w.ParentSqoTypeInfo.Type);

                if (i > 0)
                {
                    var and = new And();
                    and.Add(w, wPrev);

                    wPrev = and;
                }
                else
                {
                    wPrev = w;
                }

                i++;
            }

            var oids = await wPrev.GetOIDsAsync().ConfigureAwait(false);
            if (oids.Count > 1)
                throw new SiaqodbException("In database exists more than one object with value of fields specified");

            if (oids.Count == 1)
            {
                objInfo.Oid = oids[0];

                if (transact == null)
                    await SaveObjectAsync(obj, ti, objInfo).ConfigureAwait(false);
                else
                    await SaveObjectAsync(obj, ti, objInfo, transact).ConfigureAwait(false);
                return true;
            }

            return false;
        }
#endif
        private void serializer_NeedSaveComplexObject(object sender, ComplexObjectEventArgs e)
        {
            if (e.JustSetOID)
                metaCache.SetOIDToObject(e.ObjInfo.BackendObject, e.ObjInfo.Oid, e.ObjInfo.SqoTypeInfo);
            else
                OnNeedSaveComplexObject(e);
        }
#if ASYNC
        private async Task serializer_NeedSaveComplexObjectAsync(object sender, ComplexObjectEventArgs e)
        {
            if (e.JustSetOID)
                metaCache.SetOIDToObject(e.ObjInfo.BackendObject, e.ObjInfo.Oid, e.ObjInfo.SqoTypeInfo);
            else
                await OnNeedSaveComplexObjectAsync(e).ConfigureAwait(false);
        }

#endif
        private void CheckConstraints(ObjectInfo objInfo, SqoTypeInfo ti)
        {
            if (objInfo.Oid <= 0) //if insert
                foreach (var fi in ti.UniqueFields)
                {
                    var w = new Where(fi.Name, OperationType.Equal, objInfo.AtInfo[fi]);
                    var oids = LoadFilteredOids(w, ti);
                    if (oids.Count > 0)
                        throw new UniqueConstraintException("Field:" + fi.Name + " of object with OID=" + objInfo.Oid +
                                                            " of Type:" + ti.TypeName +
                                                            "  has UniqueConstraint, duplicates not allowed!");
                }
            else //if update
                foreach (var fi in ti.UniqueFields)
                {
                    var w = new Where(fi.Name, OperationType.Equal, objInfo.AtInfo[fi]);
                    var oids = LoadFilteredOids(w, ti);
                    if (oids.Count > 0)
                    {
                        if (oids.Contains(objInfo.Oid) && oids.Count == 1) //is current one
                            continue;
                        throw new UniqueConstraintException("Field:" + fi.Name + " of object with OID=" + objInfo.Oid +
                                                            " of Type:" + ti.TypeName +
                                                            "  has UniqueConstraint, duplicates not allowed!");
                    }
                }
        }
#if ASYNC
        private async Task CheckConstraintsAsync(ObjectInfo objInfo, SqoTypeInfo ti)
        {
            if (objInfo.Oid <= 0) //if insert
                foreach (var fi in ti.UniqueFields)
                {
                    var w = new Where(fi.Name, OperationType.Equal, objInfo.AtInfo[fi]);
                    var oids = await LoadFilteredOidsAsync(w, ti).ConfigureAwait(false);
                    if (oids.Count > 0)
                        throw new UniqueConstraintException("Field:" + fi.Name + " of object with OID=" + objInfo.Oid +
                                                            " of Type:" + ti.TypeName +
                                                            "  has UniqueConstraint, duplicates not allowed!");
                }
            else //if update
                foreach (var fi in ti.UniqueFields)
                {
                    var w = new Where(fi.Name, OperationType.Equal, objInfo.AtInfo[fi]);
                    var oids = await LoadFilteredOidsAsync(w, ti).ConfigureAwait(false);
                    if (oids.Count > 0)
                    {
                        if (oids.Contains(objInfo.Oid) && oids.Count == 1) //is current one
                            continue;
                        throw new UniqueConstraintException("Field:" + fi.Name + " of object with OID=" + objInfo.Oid +
                                                            " of Type:" + ti.TypeName +
                                                            "  has UniqueConstraint, duplicates not allowed!");
                    }
                }
        }
#endif

        internal void DeleteObject(object obj, SqoTypeInfo ti)
        {
            var objInfo = MetaExtractor.GetObjectInfo(obj, ti, metaCache);

            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            lock (_syncRoot)
            {
                CheckForConcurency(obj, objInfo, ti, serializer, true);

                MarkObjectAsDelete(serializer, objInfo.Oid, ti);

                indexManager.UpdateIndexesAfterDelete(objInfo, ti);

                metaCache.SetOIDToObject(obj, -1, ti);
            }
        }
#if ASYNC
        internal async Task DeleteObjectAsync(object obj, SqoTypeInfo ti)
        {
            var objInfo = MetaExtractor.GetObjectInfo(obj, ti, metaCache);

            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            await CheckForConcurencyAsync(obj, objInfo, ti, serializer, true).ConfigureAwait(false);

            await MarkObjectAsDeleteAsync(serializer, objInfo.Oid, ti).ConfigureAwait(false);

            await indexManager.UpdateIndexesAfterDeleteAsync(objInfo, ti).ConfigureAwait(false);

            metaCache.SetOIDToObject(obj, -1, ti);
        }
#endif
        internal void DeleteObject(object obj, SqoTypeInfo ti, Transaction transact, ObjectInfo objInfo)
        {
            lock (_syncRoot)
            {
                var trObject = new TransactionObject(this);
                trObject.currentObject = obj;
                trObject.objInfo = objInfo == null ? MetaExtractor.GetObjectInfo(obj, ti, metaCache) : objInfo;
                trObject.serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

                trObject.Operation = TransactionObject.OperationType.Delete;

                CheckForConcurencyOnly(obj, trObject.objInfo, ti, trObject.serializer);

                TransactionManager.transactions[transact.ID].transactionObjects.Add(trObject);
            }
        }
#if ASYNC
        internal async Task DeleteObjectAsync(object obj, SqoTypeInfo ti, Transaction transact, ObjectInfo objInfo)
        {
            var trObject = new TransactionObject(this);
            trObject.currentObject = obj;
            trObject.objInfo = objInfo == null ? MetaExtractor.GetObjectInfo(obj, ti, metaCache) : objInfo;
            trObject.serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            trObject.Operation = TransactionObject.OperationType.Delete;

            await CheckForConcurencyOnlyAsync(obj, trObject.objInfo, ti, trObject.serializer).ConfigureAwait(false);

            TransactionManager.transactions[transact.ID].transactionObjects.Add(trObject);
        }
#endif
        internal int DeleteObjectBy(string[] fieldNames, object obj, SqoTypeInfo ti, Transaction transaction)
        {
            var objInfo = MetaExtractor.GetObjectInfo(obj, ti, metaCache);
            var i = 0;
            ICriteria wPrev = null;

            foreach (var fieldName in fieldNames)
            {
                var fi = MetaHelper.FindField(ti.Fields, fieldName);
                if (fi == null)
                    throw new SiaqodbException("Field:" + fieldName + " was not found as member of Type:" +
                                               ti.TypeName);

                var w = new Where(fieldName, OperationType.Equal, objInfo.AtInfo[fi]);
                w.StorageEngine = this;
                w.ParentSqoTypeInfo = ti;
                w.ParentType.Add(w.ParentSqoTypeInfo.Type);
                if (i > 0)
                {
                    var and = new And();
                    and.Add(w, wPrev);

                    wPrev = and;
                }
                else
                {
                    wPrev = w;
                }

                i++;
            }

            var oids = wPrev.GetOIDs();


            if (oids.Count > 1)
                throw new SiaqodbException("In database exists more than one object with value of fields specified");

            if (oids.Count == 1)
            {
                objInfo.Oid = oids[0];
                //obj.OID = oids[0];
                metaCache.SetOIDToObject(obj, oids[0], ti);

                var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
                lock (_syncRoot)
                {
                    if (transaction == null)
                    {
                        CheckForConcurency(obj, objInfo, ti, serializer, true);

                        MarkObjectAsDelete(serializer, objInfo.Oid, ti);

                        indexManager.UpdateIndexesAfterDelete(objInfo, ti);
                    }
                    else
                    {
                        DeleteObject(obj, ti, transaction, objInfo);
                    }
                }

                return oids[0];
            }

            return -1;
        }
#if ASYNC
        internal async Task<int> DeleteObjectByAsync(string[] fieldNames, object obj, SqoTypeInfo ti,
            Transaction transaction)
        {
            var objInfo = MetaExtractor.GetObjectInfo(obj, ti, metaCache);
            var i = 0;
            ICriteria wPrev = null;

            foreach (var fieldName in fieldNames)
            {
                var fi = MetaHelper.FindField(ti.Fields, fieldName);
                if (fi == null)
                    throw new SiaqodbException("Field:" + fieldName + " was not found as member of Type:" +
                                               ti.TypeName);

                var w = new Where(fieldName, OperationType.Equal, objInfo.AtInfo[fi]);
                w.StorageEngine = this;
                w.ParentSqoTypeInfo = ti;
                w.ParentType.Add(w.ParentSqoTypeInfo.Type);
                if (i > 0)
                {
                    var and = new And();
                    and.Add(w, wPrev);

                    wPrev = and;
                }
                else
                {
                    wPrev = w;
                }

                i++;
            }

            var oids = await wPrev.GetOIDsAsync().ConfigureAwait(false);


            if (oids.Count > 1)
                throw new SiaqodbException("In database exists more than one object with value of fields specified");

            if (oids.Count == 1)
            {
                objInfo.Oid = oids[0];
                //obj.OID = oids[0];
                metaCache.SetOIDToObject(obj, oids[0], ti);

                var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

                if (transaction == null)
                {
                    await CheckForConcurencyAsync(obj, objInfo, ti, serializer, true).ConfigureAwait(false);

                    await MarkObjectAsDeleteAsync(serializer, objInfo.Oid, ti).ConfigureAwait(false);

                    await indexManager.UpdateIndexesAfterDeleteAsync(objInfo, ti).ConfigureAwait(false);
                }
                else
                {
                    await DeleteObjectAsync(obj, ti, transaction, objInfo).ConfigureAwait(false);
                }

                return oids[0];
            }

            return -1;
        }
#endif
        internal List<int> DeleteObjectBy(SqoTypeInfo ti, Dictionary<string, object> criteria)
        {
            var i = 0;
            ICriteria wPrev = null;

            foreach (var fieldName in criteria.Keys)
            {
                var fi = MetaHelper.FindField(ti.Fields, fieldName);
                if (fi == null)
                    throw new SiaqodbException("Field:" + fieldName + " was not found as member of Type:" +
                                               ti.TypeName);

                var w = new Where(fieldName, OperationType.Equal, criteria[fieldName]);
                w.StorageEngine = this;
                w.ParentSqoTypeInfo = ti;
                w.ParentType.Add(w.ParentSqoTypeInfo.Type);
                if (i > 0)
                {
                    var and = new And();
                    and.Add(w, wPrev);

                    wPrev = and;
                }
                else
                {
                    wPrev = w;
                }

                i++;
            }

            var oids = wPrev.GetOIDs();

            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            lock (_syncRoot)
            {
                foreach (var oid in oids)
                {
                    MarkObjectAsDelete(serializer, oid, ti);

                    indexManager.UpdateIndexesAfterDelete(oid, ti);
                }
            }

            return oids;
        }
#if ASYNC
        internal async Task<List<int>> DeleteObjectByAsync(SqoTypeInfo ti, Dictionary<string, object> criteria)
        {
            var i = 0;
            ICriteria wPrev = null;

            foreach (var fieldName in criteria.Keys)
            {
                var fi = MetaHelper.FindField(ti.Fields, fieldName);
                if (fi == null)
                    throw new SiaqodbException("Field:" + fieldName + " was not found as member of Type:" +
                                               ti.TypeName);

                var w = new Where(fieldName, OperationType.Equal, criteria[fieldName]);
                w.StorageEngine = this;
                w.ParentSqoTypeInfo = ti;
                w.ParentType.Add(w.ParentSqoTypeInfo.Type);
                if (i > 0)
                {
                    var and = new And();
                    and.Add(w, wPrev);

                    wPrev = and;
                }
                else
                {
                    wPrev = w;
                }

                i++;
            }

            var oids = await wPrev.GetOIDsAsync().ConfigureAwait(false);

            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            foreach (var oid in oids)
            {
                await MarkObjectAsDeleteAsync(serializer, oid, ti).ConfigureAwait(false);

                await indexManager.UpdateIndexesAfterDeleteAsync(oid, ti).ConfigureAwait(false);
            }


            return oids;
        }
#endif
        internal void DeleteObjectByOID(int oid, SqoTypeInfo tinf)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(tinf), useElevatedTrust);
            MarkObjectAsDelete(serializer, oid, tinf);
        }
#if ASYNC
        internal async Task DeleteObjectByOIDAsync(int oid, SqoTypeInfo tinf)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(tinf), useElevatedTrust);
            await MarkObjectAsDeleteAsync(serializer, oid, tinf).ConfigureAwait(false);
        }
#endif
        private void CheckForConcurency(object oi, ObjectInfo objInfo, SqoTypeInfo ti, ObjectSerializer serializer,
            bool updateTickCountInDB)
        {
            if (SiaqodbConfigurator.OptimisticConcurrencyEnabled)
            {
                var fi = MetaHelper.FindField(ti.Fields, "tickCount");
                if (fi != null)
                    if (fi.AttributeType == typeof(ulong))
                    {
                        ulong tickCount = 0;

                        if (objInfo.Oid > 0 && objInfo.Oid <= ti.Header.numberOfRecords) //update or delete
                        {
                            tickCount = (ulong)serializer.ReadFieldValue(ti, objInfo.Oid, fi);
                            if (objInfo.TickCount != 0)
                                if (tickCount != objInfo.TickCount)
                                    throw new OptimisticConcurrencyException("Another version of object with OID=" +
                                                                             objInfo.Oid + " of Type:" + ti.TypeName +
                                                                             " is saved in database, refresh your object before save!");
                        }

                        tickCount = tickCount + 1L;
                        objInfo.AtInfo[fi] = tickCount;
#if SILVERLIGHT
                    MetaHelper.CallSetValue(fi.FInfo, tickCount, oi, ti.Type);
#else
                        fi.FInfo.SetValue(oi, tickCount);
#endif

                        if (updateTickCountInDB)
                            serializer.SaveFieldValue(objInfo.Oid, "tickCount", ti, tickCount, rawSerializer);
                    }
            }
        }
#if ASYNC
        private async Task CheckForConcurencyAsync(object oi, ObjectInfo objInfo, SqoTypeInfo ti,
            ObjectSerializer serializer, bool updateTickCountInDB)
        {
            if (SiaqodbConfigurator.OptimisticConcurrencyEnabled)
            {
                var fi = MetaHelper.FindField(ti.Fields, "tickCount");
                if (fi != null)
                    if (fi.AttributeType == typeof(ulong))
                    {
                        ulong tickCount = 0;

                        if (objInfo.Oid > 0 && objInfo.Oid <= ti.Header.numberOfRecords) //update or delete
                        {
                            tickCount = (ulong)await serializer.ReadFieldValueAsync(ti, objInfo.Oid, fi)
                                .ConfigureAwait(false);
                            if (objInfo.TickCount != 0)
                                if (tickCount != objInfo.TickCount)
                                    throw new OptimisticConcurrencyException("Another version of object with OID=" +
                                                                             objInfo.Oid + " of Type:" + ti.TypeName +
                                                                             " is saved in database, refresh your object before save!");
                        }

                        tickCount = tickCount + 1L;
                        objInfo.AtInfo[fi] = tickCount;
#if SILVERLIGHT
                    MetaHelper.CallSetValue(fi.FInfo, tickCount, oi, ti.Type);
#else
                        fi.FInfo.SetValue(oi, tickCount);
#endif

                        if (updateTickCountInDB)
                            await serializer.SaveFieldValueAsync(objInfo.Oid, "tickCount", ti, tickCount, rawSerializer)
                                .ConfigureAwait(false);
                    }
            }
        }
#endif
        private void CheckForConcurencyOnly(object oi, ObjectInfo objInfo, SqoTypeInfo ti, ObjectSerializer serializer)
        {
            if (SiaqodbConfigurator.OptimisticConcurrencyEnabled)
            {
                var fi = MetaHelper.FindField(ti.Fields, "tickCount");
                if (fi != null)
                    if (fi.AttributeType == typeof(ulong))
                    {
                        ulong tickCount = 0;

                        if (objInfo.Oid > 0 && objInfo.Oid <= ti.Header.numberOfRecords) //update or delete
                        {
                            tickCount = (ulong)serializer.ReadFieldValue(ti, objInfo.Oid, fi);
                            if (objInfo.TickCount != 0)
                                if (tickCount != objInfo.TickCount)
                                    throw new OptimisticConcurrencyException("Another version of object with OID=" +
                                                                             objInfo.Oid + " of Type:" + ti.TypeName +
                                                                             " is saved in database, refresh your object before save!");
                        }
                    }
            }
        }
#if ASYNC
        private async Task CheckForConcurencyOnlyAsync(object oi, ObjectInfo objInfo, SqoTypeInfo ti,
            ObjectSerializer serializer)
        {
            if (SiaqodbConfigurator.OptimisticConcurrencyEnabled)
            {
                var fi = MetaHelper.FindField(ti.Fields, "tickCount");
                if (fi != null)
                    if (fi.AttributeType == typeof(ulong))
                    {
                        ulong tickCount = 0;

                        if (objInfo.Oid > 0 && objInfo.Oid <= ti.Header.numberOfRecords) //update or delete
                        {
                            tickCount = (ulong)await serializer.ReadFieldValueAsync(ti, objInfo.Oid, fi)
                                .ConfigureAwait(false);
                            if (objInfo.TickCount != 0)
                                if (tickCount != objInfo.TickCount)
                                    throw new OptimisticConcurrencyException("Another version of object with OID=" +
                                                                             objInfo.Oid + " of Type:" + ti.TypeName +
                                                                             " is saved in database, refresh your object before save!");
                        }
                    }
            }
        }
#endif
        internal int AllocateNewOID(SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.SaveNrRecords(ti, ti.Header.numberOfRecords + 1);
            return ti.Header.numberOfRecords;
        }
#if ASYNC
        internal async Task<int> AllocateNewOIDAsync(SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            await serializer.SaveNrRecordsAsync(ti, ti.Header.numberOfRecords + 1).ConfigureAwait(false);
            return ti.Header.numberOfRecords;
        }
#endif
        private void MarkObjectAsDelete(ObjectSerializer serializer, int oid, SqoTypeInfo ti)
        {
            foreach (var ai in ti.Fields)
            {
                var byteTrans = ByteTransformerFactory.GetByteTransformer(null, null, ai, ti);
                if (byteTrans is ArrayByteTranformer || byteTrans is DictionaryByteTransformer)
                {
                    var arrayInfo = GetArrayMetaOfField(ti, oid, ai);
                    if (arrayInfo.Name > 0)
                        rawSerializer
                            .MarkRawInfoAsFree(arrayInfo
                                .Name); //this helps Shrink method to detect unused rawdata blocks.
                }
            }

            serializer.MarkObjectAsDelete(oid, ti);
        }
#if ASYNC
        private async Task MarkObjectAsDeleteAsync(ObjectSerializer serializer, int oid, SqoTypeInfo ti)
        {
            foreach (var ai in ti.Fields)
            {
                var byteTrans = ByteTransformerFactory.GetByteTransformer(null, null, ai, ti);
                if (byteTrans is ArrayByteTranformer || byteTrans is DictionaryByteTransformer)
                {
                    var arrayInfo = await GetArrayMetaOfFieldAsync(ti, oid, ai).ConfigureAwait(false);
                    if (arrayInfo.Name > 0)
                        await rawSerializer.MarkRawInfoAsFreeAsync(arrayInfo.Name)
                            .ConfigureAwait(false); //this helps Shrink method to detect unused rawdata blocks.
                }
            }

            await serializer.MarkObjectAsDeleteAsync(oid, ti).ConfigureAwait(false);
        }
#endif
        private void MarkFreeSpace(SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            var nrRecords = ti.Header.numberOfRecords;
            var existingDynamicFields = new List<FieldSqoInfo>();
            foreach (var ai in ti.Fields)
            {
                var byteTrans = ByteTransformerFactory.GetByteTransformer(null, null, ai, ti);
                if (byteTrans is ArrayByteTranformer || byteTrans is DictionaryByteTransformer)
                    existingDynamicFields.Add(ai);
            }

            if (existingDynamicFields.Count > 0)
                for (var i = 0; i < nrRecords; i++)
                {
                    var oid = i + 1;
                    foreach (var ai in existingDynamicFields)
                    {
                        var arrayInfo = GetArrayMetaOfField(ti, oid, ai);
                        if (arrayInfo.Name > 0)
                            rawSerializer
                                .MarkRawInfoAsFree(arrayInfo
                                    .Name); //this helps Shrink method to detect unused rawdata blocks.
                    }
                }
        }
#if ASYNC
        private async Task MarkFreeSpaceAsync(SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            var nrRecords = ti.Header.numberOfRecords;
            var existingDynamicFields = new List<FieldSqoInfo>();
            foreach (var ai in ti.Fields)
            {
                var byteTrans = ByteTransformerFactory.GetByteTransformer(null, null, ai, ti);
                if (byteTrans is ArrayByteTranformer || byteTrans is DictionaryByteTransformer)
                    existingDynamicFields.Add(ai);
            }

            if (existingDynamicFields.Count > 0)
                for (var i = 0; i < nrRecords; i++)
                {
                    var oid = i + 1;
                    foreach (var ai in existingDynamicFields)
                    {
                        var arrayInfo = await GetArrayMetaOfFieldAsync(ti, oid, ai).ConfigureAwait(false);
                        if (arrayInfo.Name > 0)
                            await rawSerializer.MarkRawInfoAsFreeAsync(arrayInfo.Name)
                                .ConfigureAwait(false); //this helps Shrink method to detect unused rawdata blocks.
                    }
                }
        }
#endif
        internal void MarkRawInfoAsFree(List<int> rawdataInfoOIDs)
        {
            foreach (var oid in
                     rawdataInfoOIDs)
                rawSerializer.MarkRawInfoAsFree(oid); //this helps Shrink method to detect unused rawdata blocks.
        }
#if ASYNC
        internal async Task MarkRawInfoAsFreeAsync(List<int> rawdataInfoOIDs)
        {
            foreach (var oid in
                     rawdataInfoOIDs)
                await rawSerializer.MarkRawInfoAsFreeAsync(oid)
                    .ConfigureAwait(false); //this helps Shrink method to detect unused rawdata blocks.
        }
#endif
        internal void SetFileLength(long newLength, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.SetLength(newLength);
        }

        internal int SaveObjectBytes(byte[] objBytes, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return serializer.SerializeObjectWithNewOID(objBytes, ti);
        }
#if ASYNC
        internal async Task<int> SaveObjectBytesAsync(byte[] objBytes, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return await serializer.SerializeObjectWithNewOIDAsync(objBytes, ti).ConfigureAwait(false);
        }
#endif
        internal void AdjustComplexFieldsAfterShrink(SqoTypeInfo ti, IList<ShrinkResult> shrinkResults)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            var complexFields = (from FieldSqoInfo fi in ti.Fields
                where fi.AttributeTypeId == MetaExtractor.complexID || fi.AttributeTypeId == MetaExtractor.documentID
                select fi).ToList();
            if (complexFields.Count > 0)
                foreach (var fi in complexFields)
                {
                    var oldOidNewOid = new Dictionary<int, int>();
                    var nrRecords = ti.Header.numberOfRecords;
                    var k = 0;
                    for (var i = 0; i < nrRecords; i++)
                    {
                        var oid = i + 1;
                        var Oid_Tid = serializer.ReadOIDAndTID(ti, oid, fi);
                        if (Oid_Tid.Key == 0 && Oid_Tid.Value == 0) //mean complex object is null
                            continue;
                        if (k == 0)
                        {
                            var shrinkResultsFiltered = from ShrinkResult shrinkRes in shrinkResults
                                where shrinkRes.TID == Oid_Tid.Value
                                select shrinkRes;

                            foreach (var shF in shrinkResultsFiltered) oldOidNewOid[shF.Old_OID] = shF.New_OID;
                        }

                        if (oldOidNewOid.ContainsKey(Oid_Tid.Key))
                        {
                            var newOid = oldOidNewOid[Oid_Tid.Key];

                            serializer.SaveComplexFieldContent(new KeyValuePair<int, int>(newOid, Oid_Tid.Value), fi,
                                ti, oid);
                        }

                        k++;
                    }
                }
        }
#if ASYNC
        internal async Task AdjustComplexFieldsAfterShrinkAsync(SqoTypeInfo ti, IList<ShrinkResult> shrinkResults)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            var complexFields = (from FieldSqoInfo fi in ti.Fields
                where fi.AttributeTypeId == MetaExtractor.complexID
                select fi).ToList();
            if (complexFields.Count > 0)
                foreach (var fi in complexFields)
                {
                    var oldOidNewOid = new Dictionary<int, int>();
                    var nrRecords = ti.Header.numberOfRecords;
                    var k = 0;
                    for (var i = 0; i < nrRecords; i++)
                    {
                        var oid = i + 1;
                        var Oid_Tid = await serializer.ReadOIDAndTIDAsync(ti, oid, fi).ConfigureAwait(false);
                        if (Oid_Tid.Key == 0 && Oid_Tid.Value == 0) //mean complex object is null
                            continue;
                        if (k == 0)
                        {
                            var shrinkResultsFiltered = from ShrinkResult shrinkRes in shrinkResults
                                where shrinkRes.TID == Oid_Tid.Value
                                select shrinkRes;

                            foreach (var shF in shrinkResultsFiltered) oldOidNewOid[shF.Old_OID] = shF.New_OID;
                        }

                        if (oldOidNewOid.ContainsKey(Oid_Tid.Key))
                        {
                            var newOid = oldOidNewOid[Oid_Tid.Key];

                            await serializer
                                .SaveComplexFieldContentAsync(new KeyValuePair<int, int>(newOid, Oid_Tid.Value), fi, ti,
                                    oid).ConfigureAwait(false);
                        }

                        k++;
                    }
                }
        }
#endif
        internal void AdjustArrayFieldsAfterShrink(SqoTypeInfo ti, FieldSqoInfo fi, int objectOID, int newOID)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.SaveArrayOIDFieldContent(ti, fi, objectOID, newOID);
        }
    }
}