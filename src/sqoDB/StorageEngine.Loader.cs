using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using sqoDB.Core;
using sqoDB.Exceptions;
using sqoDB.Meta;
using sqoDB.Queries;
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
        internal ObjectList<T> LoadAll<T>(SqoTypeInfo ti)
        {
            var ol = new ObjectList<T>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObject += serializer_NeedReadComplexObject;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;
            var nrRecords = ti.Header.numberOfRecords;
            var rangeSize = Convert.ToInt32(SiaqodbConfigurator.BufferingChunkPercent * nrRecords / 100);
            if (rangeSize < 1) rangeSize = 1;

            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (i % rangeSize == 0)
                {
                    var oidEnd = i + rangeSize <= nrRecords ? i + rangeSize : nrRecords;
                    serializer.PreLoadBytes(oid, oidEnd, ti);
                }

                if (serializer.IsObjectDeleted(oid, ti)) continue;
                if (SiaqodbConfigurator.RaiseLoadEvents)
                {
                    var args = new LoadingObjectEventArgs(oid, ti.Type);
                    OnLoadingObject(args);
                    if (args.Cancel) continue;

                    if (args.Replace != null)
                    {
                        ol.Add((T)args.Replace);
                        continue;
                    }
                }

                var currentObj = default(T);
                currentObj = Activator.CreateInstance<T>();
                circularRefCache.Clear();
                circularRefCache.Add(oid, ti, currentObj);
                try
                {
                    serializer.ReadObject(currentObj, ti, oid, rawSerializer);
                }
                catch (ArgumentException ex)
                {
                    SiaqodbConfigurator.LogMessage("Object with OID:" + oid + " seems to be corrupted!",
                        VerboseLevel.Error);

                    if (SiaqodbUtil.IsRepairMode)
                    {
                        SiaqodbConfigurator.LogMessage("Object with OID:" + oid + " is deleted", VerboseLevel.Warn);
                        DeleteObjectByOID(oid, ti);
                        continue;
                    }

                    throw ex;
                }

                metaCache.SetOIDToObject(currentObj, oid, ti);
                if (SiaqodbConfigurator.RaiseLoadEvents) OnLoadedObject(oid, currentObj);
                ol.Add(currentObj);
            }

            serializer.ResetPreload();
            return ol;
        }
#if ASYNC
        internal async Task<ObjectList<T>> LoadAllAsync<T>(SqoTypeInfo ti)
        {
            var ol = new ObjectList<T>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObjectAsync += serializer_NeedReadComplexObjectAsync;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            var nrRecords = ti.Header.numberOfRecords;
            var rangeSize = Convert.ToInt32(SiaqodbConfigurator.BufferingChunkPercent * nrRecords / 100);
            if (rangeSize < 1) rangeSize = 1;

            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (i % rangeSize == 0)
                {
                    var oidEnd = i + rangeSize <= nrRecords ? i + rangeSize : nrRecords;
                    await serializer.PreLoadBytesAsync(oid, oidEnd, ti).ConfigureAwait(false);
                }

                if (await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false)) continue;
                if (SiaqodbConfigurator.RaiseLoadEvents)
                {
                    var args = new LoadingObjectEventArgs(oid, ti.Type);
                    OnLoadingObject(args);
                    if (args.Cancel) continue;

                    if (args.Replace != null)
                    {
                        ol.Add((T)args.Replace);
                        continue;
                    }
                }

                var currentObj = default(T);
                currentObj = Activator.CreateInstance<T>();
                circularRefCache.Clear();
                circularRefCache.Add(oid, ti, currentObj);
                var exTh = false;
                try
                {
                    await serializer.ReadObjectAsync(currentObj, ti, oid, rawSerializer).ConfigureAwait(false);
                }
                catch (ArgumentException ex)
                {
                    SiaqodbConfigurator.LogMessage("Object with OID:" + oid + " seems to be corrupted!",
                        VerboseLevel.Error);

                    if (SiaqodbUtil.IsRepairMode)
                    {
                        SiaqodbConfigurator.LogMessage("Object with OID:" + oid + " is deleted", VerboseLevel.Warn);
                        exTh = true;
                    }
                    else
                    {
                        throw ex;
                    }
                }

                if (exTh)
                {
                    await DeleteObjectByOIDAsync(oid, ti).ConfigureAwait(false);
                    continue;
                }

                metaCache.SetOIDToObject(currentObj, oid, ti);
                if (SiaqodbConfigurator.RaiseLoadEvents) OnLoadedObject(oid, currentObj);
                ol.Add(currentObj);
            }

            serializer.ResetPreload();
            return ol;
        }
#endif
        internal ObjectTable LoadAll(SqoTypeInfo ti)
        {
            var obTable = new ObjectTable();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObject += serializer_NeedReadComplexObject;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            var nrRecords = ti.Header.numberOfRecords;
            var rangeSize = Convert.ToInt32(SiaqodbConfigurator.BufferingChunkPercent * nrRecords / 100);
            if (rangeSize < 1) rangeSize = 1;

            obTable.Columns.Add("OID", 0);
            var j = 1;
            foreach (var fi in ti.Fields)
            {
                obTable.Columns.Add(fi.Name, j);
                j++;
            }

            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (i % rangeSize == 0)
                {
                    var oidEnd = i + rangeSize <= nrRecords ? i + rangeSize : nrRecords;
                    serializer.PreLoadBytes(oid, oidEnd, ti);
                }

                if (serializer.IsObjectDeleted(oid, ti))
                {
                    var row = obTable.NewRow();
                    row["OID"] = -oid;
                    obTable.Rows.Add(row);
                }
                else
                {
                    var row = obTable.NewRow();
                    row["OID"] = oid;
                    serializer.ReadObjectRow(row, ti, oid, rawSerializer);

                    obTable.Rows.Add(row);
                }
            }

            serializer.ResetPreload();
            return obTable;
        }
#if ASYNC
        internal async Task<ObjectTable> LoadAllAsync(SqoTypeInfo ti)
        {
            var obTable = new ObjectTable();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObjectAsync += serializer_NeedReadComplexObjectAsync;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            var nrRecords = ti.Header.numberOfRecords;
            var rangeSize = Convert.ToInt32(SiaqodbConfigurator.BufferingChunkPercent * nrRecords / 100);
            if (rangeSize < 1) rangeSize = 1;

            obTable.Columns.Add("OID", 0);
            var j = 1;
            foreach (var fi in ti.Fields)
            {
                obTable.Columns.Add(fi.Name, j);
                j++;
            }

            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (i % rangeSize == 0)
                {
                    var oidEnd = i + rangeSize <= nrRecords ? i + rangeSize : nrRecords;
                    await serializer.PreLoadBytesAsync(oid, oidEnd, ti).ConfigureAwait(false);
                }

                if (await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false))
                {
                    var row = obTable.NewRow();
                    row["OID"] = -oid;
                    obTable.Rows.Add(row);
                }
                else
                {
                    var row = obTable.NewRow();
                    row["OID"] = oid;
                    await serializer.ReadObjectRowAsync(row, ti, oid, rawSerializer).ConfigureAwait(false);

                    obTable.Rows.Add(row);
                }
            }

            serializer.ResetPreload();
            return obTable;
        }

#endif
        internal List<int> LoadFilteredOids(Where where)
        {
            List<int> oids = null;


            //fix Types problem when a field is declared in a base class and used in a derived class
            var type = where.ParentSqoTypeInfo.Type;

            for (var j = where.AttributeName.Count - 1; j >= 0; j--)
            {
                var fieldName = where.AttributeName[j];
                var finfo = MetaExtractor.FindField(type, fieldName);
                if (finfo != null)
                {
                    where.ParentType[j] = type;
                    type = finfo.FieldType;
                }
                else if (fieldName == "OID")
                {
                    where.ParentType[j] = type;
                }
            }

            var i = 0;
            foreach (var attName in where.AttributeName)
            {
                if (i == 0) //the deepest property
                {
                    var ti = GetSqoTypeInfoSoft(where.ParentType[i]);
                    oids = LoadFilteredOids(where, ti);
                }
                else
                {
                    var ti = GetSqoTypeInfoSoft(where.ParentType[i]);
                    var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
                    var oidsComplextObj = GetOIDsOfComplexObj(ti, where.AttributeName[i], oids);
                    oids = oidsComplextObj;
                }

                i++;
            }

            return oids;
        }

#if ASYNC
        internal async Task<List<int>> LoadFilteredOidsAsync(Where where)
        {
            List<int> oids = null;


            //fix Types problem when a field is declared in a base class and used in a derived class
            var type = where.ParentSqoTypeInfo.Type;

            for (var j = where.AttributeName.Count - 1; j >= 0; j--)
            {
                var fieldName = where.AttributeName[j];
                var finfo = MetaExtractor.FindField(type, fieldName);
                if (finfo != null)
                {
                    where.ParentType[j] = type;
                    type = finfo.FieldType;
                }
                else if (fieldName == "OID")
                {
                    where.ParentType[j] = type;
                }
            }

            var i = 0;
            foreach (var attName in where.AttributeName)
            {
                if (i == 0) //the deepest property
                {
                    var ti = GetSqoTypeInfoSoft(where.ParentType[i]);
                    oids = await LoadFilteredOidsAsync(where, ti).ConfigureAwait(false);
                }
                else
                {
                    var ti = GetSqoTypeInfoSoft(where.ParentType[i]);
                    var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
                    var oidsComplextObj = await GetOIDsOfComplexObjAsync(ti, where.AttributeName[i], oids)
                        .ConfigureAwait(false);
                    oids = oidsComplextObj;
                }

                i++;
            }

            return oids;
        }

#endif
        private List<int> GetOIDsOfComplexObj(SqoTypeInfo ti, string fieldName, List<int> insideOids)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            insideOids.Sort();
            var nrRecords = ti.Header.numberOfRecords;
            var oids = new List<int>();
            if (insideOids.Count == 0)
                return oids;

            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (serializer.IsObjectDeleted(oid, ti)) continue;

                var oidOfComplex = serializer.ReadOidOfComplex(ti, oid, fieldName, rawSerializer);

                var index = insideOids.BinarySearch(oidOfComplex); //intersection
                if (index >= 0) oids.Add(oid);
            }

            return oids;
        }
#if ASYNC
        private async Task<List<int>> GetOIDsOfComplexObjAsync(SqoTypeInfo ti, string fieldName, List<int> insideOids)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            insideOids.Sort();
            var nrRecords = ti.Header.numberOfRecords;
            var oids = new List<int>();
            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false)) continue;

                var oidOfComplex = await serializer.ReadOidOfComplexAsync(ti, oid, fieldName, rawSerializer)
                    .ConfigureAwait(false);

                var index = insideOids.BinarySearch(oidOfComplex); //intersection
                if (index >= 0) oids.Add(oid);
            }

            return oids;
        }
#endif
        internal List<int> LoadFilteredOids(Where where, SqoTypeInfo ti)
        {
            var oids = new List<int>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObject += serializer_NeedReadComplexObject;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            var nrRecords = ti.Header.numberOfRecords;
            var isOIDField = where.AttributeName[0] == "OID";
            if (!indexManager.LoadOidsByIndex(ti, where.AttributeName[0], where, oids))
            {
                if (isOIDField)
                {
                    FillOidsIndexed(oids, where, ti, serializer);
                }
                else //full scann
                {
                    var rangeSize = Convert.ToInt32(SiaqodbConfigurator.BufferingChunkPercent * nrRecords / 100);
                    if (rangeSize < 1) rangeSize = 1;

                    for (var i = 0; i < nrRecords; i++)
                    {
                        var oid = i + 1;
                        if (i % rangeSize == 0)
                        {
                            var oidEnd = i + rangeSize <= nrRecords ? i + rangeSize : nrRecords;
                            serializer.PreLoadBytes(oid, oidEnd, ti);
                        }

                        if (serializer.IsObjectDeleted(oid, ti)) continue;

                        var val = serializer.ReadFieldValue(ti, oid, where.AttributeName[0], rawSerializer);
                        if (Match(where, val)) oids.Add(oid);
                    }

                    serializer.ResetPreload();
                }
            }

            return oids;
        }

#if ASYNC
        internal async Task<List<int>> LoadFilteredOidsAsync(Where where, SqoTypeInfo ti)
        {
            var oids = new List<int>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObjectAsync += serializer_NeedReadComplexObjectAsync;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            var nrRecords = ti.Header.numberOfRecords;
            var isOIDField = where.AttributeName[0] == "OID";
            if (!await indexManager.LoadOidsByIndexAsync(ti, where.AttributeName[0], where, oids).ConfigureAwait(false))
            {
                if (isOIDField)
                {
                    await FillOidsIndexedAsync(oids, where, ti, serializer).ConfigureAwait(false);
                }
                else //full scann
                {
                    var rangeSize = Convert.ToInt32(SiaqodbConfigurator.BufferingChunkPercent * nrRecords / 100);
                    if (rangeSize < 1) rangeSize = 1;

                    for (var i = 0; i < nrRecords; i++)
                    {
                        var oid = i + 1;
                        if (i % rangeSize == 0)
                        {
                            var oidEnd = i + rangeSize <= nrRecords ? i + rangeSize : nrRecords;
                            await serializer.PreLoadBytesAsync(oid, oidEnd, ti).ConfigureAwait(false);
                        }

                        if (await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false)) continue;

                        var val = await serializer.ReadFieldValueAsync(ti, oid, where.AttributeName[0], rawSerializer)
                            .ConfigureAwait(false);
                        if (Match(where, val)) oids.Add(oid);
                    }

                    serializer.ResetPreload();
                }
            }

            return oids;
        }
#endif


        private void FillOidsIndexed(List<int> oids, Where where, SqoTypeInfo ti, ObjectSerializer serializer)
        {
            var oid = (int)where.Value;
            var nrRecords = ti.Header.numberOfRecords;
            if (where.OperationType == OperationType.Equal)
            {
                if (!serializer.IsObjectDeleted(oid, ti)) oids.Add(oid);
            }
            else if (where.OperationType == OperationType.NotEqual)
            {
                for (var i = 0; i < nrRecords; i++)
                {
                    var localOid = i + 1;
                    if (serializer.IsObjectDeleted(localOid, ti) || oid == localOid) continue;
                    oids.Add(localOid);
                }
            }
            else if (where.OperationType == OperationType.LessThan)
            {
                for (var i = 0; i < oid - 1; i++)
                {
                    var localOid = i + 1;
                    if (serializer.IsObjectDeleted(localOid, ti)) continue;
                    oids.Add(localOid);
                }
            }
            else if (where.OperationType == OperationType.LessThanOrEqual)
            {
                for (var i = 0; i < oid; i++)
                {
                    var localOid = i + 1;
                    if (serializer.IsObjectDeleted(localOid, ti)) continue;
                    oids.Add(localOid);
                }
            }
            else if (where.OperationType == OperationType.GreaterThan)
            {
                for (var i = oid; i < nrRecords; i++)
                {
                    var localOid = i + 1;
                    if (serializer.IsObjectDeleted(localOid, ti)) continue;
                    oids.Add(localOid);
                }
            }
            else if (where.OperationType == OperationType.GreaterThanOrEqual)
            {
                for (var i = oid - 1; i < nrRecords; i++)
                {
                    var localOid = i + 1;
                    if (serializer.IsObjectDeleted(localOid, ti)) continue;
                    oids.Add(localOid);
                }
            }
        }
#if ASYNC
        private async Task FillOidsIndexedAsync(List<int> oids, Where where, SqoTypeInfo ti,
            ObjectSerializer serializer)
        {
            var oid = (int)where.Value;
            var nrRecords = ti.Header.numberOfRecords;
            if (where.OperationType == OperationType.Equal)
            {
                if (!await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false)) oids.Add(oid);
            }
            else if (where.OperationType == OperationType.NotEqual)
            {
                for (var i = 0; i < nrRecords; i++)
                {
                    var localOid = i + 1;
                    if (await serializer.IsObjectDeletedAsync(localOid, ti).ConfigureAwait(false) ||
                        oid == localOid) continue;
                    oids.Add(localOid);
                }
            }
            else if (where.OperationType == OperationType.LessThan)
            {
                for (var i = 0; i < oid - 1; i++)
                {
                    var localOid = i + 1;
                    if (await serializer.IsObjectDeletedAsync(localOid, ti).ConfigureAwait(false)) continue;
                    oids.Add(localOid);
                }
            }
            else if (where.OperationType == OperationType.LessThanOrEqual)
            {
                for (var i = 0; i < oid; i++)
                {
                    var localOid = i + 1;
                    if (await serializer.IsObjectDeletedAsync(localOid, ti).ConfigureAwait(false)) continue;
                    oids.Add(localOid);
                }
            }
            else if (where.OperationType == OperationType.GreaterThan)
            {
                for (var i = oid; i < nrRecords; i++)
                {
                    var localOid = i + 1;
                    if (await serializer.IsObjectDeletedAsync(localOid, ti).ConfigureAwait(false)) continue;
                    oids.Add(localOid);
                }
            }
            else if (where.OperationType == OperationType.GreaterThanOrEqual)
            {
                for (var i = oid - 1; i < nrRecords; i++)
                {
                    var localOid = i + 1;
                    if (await serializer.IsObjectDeletedAsync(localOid, ti).ConfigureAwait(false)) continue;
                    oids.Add(localOid);
                }
            }
        }
#endif
        private bool Match(Where w, object val)
        {
            if (val == null || w.Value == null)
            {
                if (w.OperationType == OperationType.Equal)
                    return val == w.Value;
                if (w.OperationType == OperationType.NotEqual) return val != w.Value;
            }
            else
            {
                #region IList

                if (val is IList)
                {
                    if (w.OperationType == OperationType.Contains)
                    {
                        var valObj = val as IList;
                        var valWhere = w.Value as IList;
                        var valComp = w.Value as IComparable;

                        if (valComp == null && val.GetType().IsClass() && valWhere == null) //complex type
                        {
                            if (parentsComparison == null) parentsComparison = new List<object>();
                            foreach (var listObj in valObj)
                                try
                                {
                                    parentsComparison.Add(listObj);
                                    parentsComparison.Add(w.Value);

                                    if (ComplexObjectsAreEqual(listObj, w.Value)) return true;
                                }
                                finally
                                {
                                    parentsComparison.Clear();
                                }

                            return false;
                        }

                        if (valWhere != null) //jagged list
                        {
                            foreach (var listObj in valObj)
                                if (ListsAreEqual((IList)listObj, valWhere))
                                    return true;
                        }
                        else
                        {
                            return valObj.Contains(w.Value);
                        }
                    }

                    return false;
                }

                #endregion

                #region dictionary

                if (val is IDictionary)
                {
                    var dictionary = val as IDictionary;

                    var valComp = w.Value as IComparable;

                    if (valComp == null && valComp.GetType().IsClass()) //complex type
                    {
                        if (parentsComparison == null) parentsComparison = new List<object>();
                        foreach (var key in dictionary.Keys)
                            try
                            {
                                parentsComparison.Add(w.Value);
                                var keyOrVal = w.OperationType == OperationType.ContainsKey ? key : dictionary[key];

                                parentsComparison.Add(keyOrVal);

                                if (ComplexObjectsAreEqual(keyOrVal, w.Value)) return true;
                            }
                            finally
                            {
                                parentsComparison.Clear();
                            }

                        return false;
                    }

                    foreach (var key in dictionary.Keys)
                        if (w.OperationType == OperationType.ContainsKey)
                        {
                            var compareResultKey = valComp.CompareTo((IComparable)key);
                            if (compareResultKey == 0) return true;
                        }
                        else if (w.OperationType == OperationType.ContainsValue)
                        {
                            var compareResultVal = valComp.CompareTo((IComparable)dictionary[key]);
                            if (compareResultVal == 0) return true;
                        }

                    return false;
                }

                #endregion

                if (val.GetType() != w.Value.GetType()) w.Value = Convertor.ChangeType(w.Value, val.GetType());
                var valComparable = val as IComparable;
                if (valComparable == null && val.GetType().IsClass()) //complex type
                    try
                    {
                        if (parentsComparison == null) parentsComparison = new List<object>();
                        parentsComparison.Add(val);
                        parentsComparison.Add(w.Value);

                        if (w.OperationType == OperationType.Equal)
                            return ComplexObjectsAreEqual(val, w.Value);
                        else if (w.OperationType == OperationType.NotEqual)
                            return !ComplexObjectsAreEqual(val, w.Value);
                    }
                    finally
                    {
                        parentsComparison.Clear();
                    }

                var compareResult = valComparable.CompareTo((IComparable)w.Value);
                if (w.OperationType == OperationType.Equal) return compareResult == 0;

                if (w.OperationType == OperationType.NotEqual) return !(compareResult == 0);

                if (w.OperationType == OperationType.LessThan) return compareResult < 0;

                if (w.OperationType == OperationType.LessThanOrEqual) return compareResult <= 0;

                if (w.OperationType == OperationType.GreaterThan) return compareResult > 0;

                if (w.OperationType == OperationType.GreaterThanOrEqual) return compareResult >= 0;

                if (w.OperationType == OperationType.Contains && val is string)
                {
                    var wVal = w.Value as string;
                    var valObj = val as string;
                    if (w.Value2 != null && w.Value2 is StringComparison)
                        return valObj.IndexOf(wVal, (StringComparison)w.Value2) != -1;
                    return valObj.Contains(wVal);
                }

                if (w.OperationType == OperationType.StartWith)
                {
                    var wVal = w.Value as string;
                    var valObj = val as string;
                    if (w.Value2 != null && w.Value2 is StringComparison)
                        return valObj.StartsWith(wVal, (StringComparison)w.Value2);
                    return valObj.StartsWith(wVal);
                }

                if (w.OperationType == OperationType.EndWith)
                {
                    var wVal = w.Value as string;
                    var valObj = val as string;
                    if (w.Value2 != null && w.Value2 is StringComparison)
                        return valObj.EndsWith(wVal, (StringComparison)w.Value2);
                    return valObj.EndsWith(wVal);
                }
            }

            return false;
        }

        private bool ListsAreEqual(IList iList, IList valWhere)
        {
            if (iList.Count != valWhere.Count) return false;
            var i = 0;
            foreach (var elem in iList)
            {
                var elemList = elem as IList;
                if (elemList != null) return ListsAreEqual(elemList, (IList)valWhere[i]);

                var valComp = valWhere[i] as IComparable;

                if (valComp == null && valWhere[i].GetType().IsClass()) //complex type
                {
                    if (!ComplexObjectsAreEqual(elem, valWhere[i]))
                        return false;
                }
                else
                {
                    if (elem.GetType() != valWhere[i].GetType())
                        valWhere[i] = Convertor.ChangeType(valWhere[i], elem.GetType());
                    var compareResult = valComp.CompareTo((IComparable)elem);
                    if (compareResult != 0) return false;
                }

                i++;
            }

            return true;
        }

        private bool ComplexObjectsAreEqual(object obj1, object obj2)
        {
            var ti = metaCache.GetSqoTypeInfo(obj1.GetType());
            foreach (var fi in ti.Fields)
            {
#if SILVERLIGHT
				object objVal1 = null;
                object objVal2 = null;
                try
				{
					objVal1 = MetaHelper.CallGetValue(fi.FInfo,obj1,ti.Type);
					objVal2 = MetaHelper.CallGetValue(fi.FInfo,obj2,ti.Type);
				}
				catch (Exception ex)
				{
					throw new SiaqodbException("Override GetValue and SetValue methods of SqoDataObject-Silverlight limitation to private fields");
				}
#else
                var objVal1 = fi.FInfo.GetValue(obj1);
                var objVal2 = fi.FInfo.GetValue(obj2);
#endif
                if (fi.FInfo.FieldType == typeof(string))
                {
                    if (objVal1 == null) objVal1 = string.Empty;
                    if (objVal2 == null) objVal2 = string.Empty;
                }

                if (objVal1 == null || objVal2 == null)
                {
                    if (objVal1 != objVal2)
                        return false;
                }
                else
                {
                    var valComparable = objVal1 as IComparable;
                    if (valComparable != null)
                    {
                        if (valComparable.CompareTo((IComparable)objVal2) != 0) return false;
                    }
                    else if (objVal1 is IList)
                    {
                        return ListsAreEqual((IList)objVal1, (IList)objVal2);
                    }
                    else if (objVal1.GetType().IsClass()) //complex type
                    {
                        if (parentsComparison.Contains(objVal1) || parentsComparison.Contains(objVal2)) continue;
                        parentsComparison.Add(objVal1);
                        parentsComparison.Add(objVal2);

                        if (!ComplexObjectsAreEqual(objVal1, objVal2)) return false;
                    }
                }
            }

            return true;
        }

        internal List<int> LoadFilteredDeletedOids(Where where, SqoTypeInfo ti)
        {
            var oids = new List<int>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            var nrRecords = ti.Header.numberOfRecords;

            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (serializer.IsObjectDeleted(oid, ti))
                {
                    var val = serializer.ReadFieldValue(ti, oid, where.AttributeName[0]);
                    if (Match(where, val)) oids.Add(oid);
                }
            }

            return oids;
        }
#if ASYNC
        internal async Task<List<int>> LoadFilteredDeletedOidsAsync(Where where, SqoTypeInfo ti)
        {
            var oids = new List<int>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            var nrRecords = ti.Header.numberOfRecords;

            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false))
                {
                    var val = await serializer.ReadFieldValueAsync(ti, oid, where.AttributeName[0])
                        .ConfigureAwait(false);
                    if (Match(where, val)) oids.Add(oid);
                }
            }

            return oids;
        }
#endif
        internal IObjectList<T> LoadByOIDs<T>(List<int> oids, SqoTypeInfo ti)
        {
            var ol = new ObjectList<T>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObject += serializer_NeedReadComplexObject;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;
            //int nrRecords = ti.Header.numberOfRecords;
            foreach (var oid in oids)
            {
                if (SiaqodbConfigurator.RaiseLoadEvents)
                {
                    var args = new LoadingObjectEventArgs(oid, ti.Type);
                    OnLoadingObject(args);
                    if (args.Cancel) continue;

                    if (args.Replace != null)
                    {
                        ol.Add((T)args.Replace);
                        continue;
                    }
                }

                var currentObj = default(T);
                currentObj = Activator.CreateInstance<T>();
                circularRefCache.Clear();
                circularRefCache.Add(oid, ti, currentObj);
                serializer.ReadObject(currentObj, ti, oid, rawSerializer);

                metaCache.SetOIDToObject(currentObj, oid, ti);
                if (SiaqodbConfigurator.RaiseLoadEvents) OnLoadedObject(oid, currentObj);
                ol.Add(currentObj);
            }

            return ol;
        }

        private void serializer_NeedCacheDocument(object sender, DocumentEventArgs e)
        {
            metaCache.AddDocument(e.TypeInfo, e.ParentObject, e.FieldName, e.DocumentInfoOID);
        }
#if ASYNC
        internal async Task<IObjectList<T>> LoadByOIDsAsync<T>(List<int> oids, SqoTypeInfo ti)
        {
            var ol = new ObjectList<T>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObjectAsync += serializer_NeedReadComplexObjectAsync;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            //int nrRecords = ti.Header.numberOfRecords;
            foreach (var oid in oids)
            {
                if (SiaqodbConfigurator.RaiseLoadEvents)
                {
                    var args = new LoadingObjectEventArgs(oid, ti.Type);
                    OnLoadingObject(args);
                    if (args.Cancel) continue;

                    if (args.Replace != null)
                    {
                        ol.Add((T)args.Replace);
                        continue;
                    }
                }

                var currentObj = default(T);
                currentObj = Activator.CreateInstance<T>();
                circularRefCache.Clear();
                circularRefCache.Add(oid, ti, currentObj);
                await serializer.ReadObjectAsync(currentObj, ti, oid, rawSerializer).ConfigureAwait(false);

                metaCache.SetOIDToObject(currentObj, oid, ti);
                if (SiaqodbConfigurator.RaiseLoadEvents) OnLoadedObject(oid, currentObj);
                ol.Add(currentObj);
            }

            return ol;
        }

#endif
        private void serializer_NeedReadComplexObject(object sender, ComplexObjectEventArgs e)
        {
            if (e.TID == 0 && e.SavedOID == 0) //means null
            {
                e.ComplexObject = null;
                return;
            }

            if (e.ParentType != null && SiaqodbConfigurator.LazyLoaded != null &&
                SiaqodbConfigurator.LazyLoaded.ContainsKey(e.ParentType))
                if (SiaqodbConfigurator.LazyLoaded[e.ParentType])
                    if (!ExistsInIncludesCache(e.ParentType, e.FieldName))
                    {
                        e.ComplexObject = null;
                        return;
                    }

            var ti = metaCache.GetSqoTypeInfoByTID(e.TID);
            var cacheObj = circularRefCache.GetObject(e.SavedOID, ti);
            if (cacheObj != null)
            {
                e.ComplexObject = cacheObj;
            }
            else
            {
                var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
                //if there is a Nested object of same type we have to reset
                serializer.ResetPreload();

                if (!IsObjectDeleted(e.SavedOID, ti)) e.ComplexObject = LoadObjectByOID(ti, e.SavedOID, false);
            }
        }
#if ASYNC
        private async Task serializer_NeedReadComplexObjectAsync(object sender, ComplexObjectEventArgs e)
        {
            if (e.TID == 0 && e.SavedOID == 0) //means null
            {
                e.ComplexObject = null;
                return;
            }

            if (e.ParentType != null && SiaqodbConfigurator.LazyLoaded != null &&
                SiaqodbConfigurator.LazyLoaded.ContainsKey(e.ParentType))
                if (SiaqodbConfigurator.LazyLoaded[e.ParentType])
                    if (!ExistsInIncludesCache(e.ParentType, e.FieldName))
                    {
                        e.ComplexObject = null;
                        return;
                    }

            var ti = metaCache.GetSqoTypeInfoByTID(e.TID);
            var cacheObj = circularRefCache.GetObject(e.SavedOID, ti);
            if (cacheObj != null)
            {
                e.ComplexObject = cacheObj;
            }
            else
            {
                var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
                //if there is a Nested object of same type we have to reset
                serializer.ResetPreload();
                if (!await IsObjectDeletedAsync(e.SavedOID, ti).ConfigureAwait(false))
                    e.ComplexObject = await LoadObjectByOIDAsync(ti, e.SavedOID, false).ConfigureAwait(false);
            }
        }
#endif
        internal List<object> LoadByOIDs(List<int> oids, SqoTypeInfo ti)
        {
            var ol = new List<object>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObject += serializer_NeedReadComplexObject;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            foreach (var oid in oids)
            {
                if (SiaqodbConfigurator.RaiseLoadEvents)
                {
                    var args = new LoadingObjectEventArgs(oid, ti.Type);
                    OnLoadingObject(args);
                    if (args.Cancel) continue;

                    if (args.Replace != null)
                    {
                        ol.Add(args.Replace);
                        continue;
                    }
                }

                var currentObj = Activator.CreateInstance(ti.Type);
                circularRefCache.Clear();
                circularRefCache.Add(oid, ti, currentObj);
                serializer.ReadObject(currentObj, ti, oid, rawSerializer);

                metaCache.SetOIDToObject(currentObj, oid, ti);
                if (SiaqodbConfigurator.RaiseLoadEvents) OnLoadedObject(oid, currentObj);
                ol.Add(currentObj);
            }

            return ol;
        }

#if ASYNC
        internal async Task<List<object>> LoadByOIDsAsync(List<int> oids, SqoTypeInfo ti)
        {
            var ol = new List<object>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObjectAsync += serializer_NeedReadComplexObjectAsync;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;


            foreach (var oid in oids)
            {
                if (SiaqodbConfigurator.RaiseLoadEvents)
                {
                    var args = new LoadingObjectEventArgs(oid, ti.Type);
                    OnLoadingObject(args);
                    if (args.Cancel) continue;

                    if (args.Replace != null)
                    {
                        ol.Add(args.Replace);
                        continue;
                    }
                }

                var currentObj = Activator.CreateInstance(ti.Type);
                circularRefCache.Clear();
                circularRefCache.Add(oid, ti, currentObj);
                await serializer.ReadObjectAsync(currentObj, ti, oid, rawSerializer).ConfigureAwait(false);

                metaCache.SetOIDToObject(currentObj, oid, ti);
                if (SiaqodbConfigurator.RaiseLoadEvents) OnLoadedObject(oid, currentObj);
                ol.Add(currentObj);
            }

            return ol;
        }

#endif

        internal List<KeyValuePair<int, int>> LoadJoin(SqoTypeInfo tiOuter, string criteriaOuter, List<int> oidOuter,
            SqoTypeInfo tiInner, string criteriaInner, List<int> oidInner)
        {
            var oids = new List<KeyValuePair<int, int>>();
            var serializerOuter = SerializerFactory.GetSerializer(path, GetFileByType(tiOuter), useElevatedTrust);
            var serializerInner = SerializerFactory.GetSerializer(path, GetFileByType(tiInner), useElevatedTrust);
            var outCheckForDeleted = false;
            var innCheckForDeleted = false;

            var nrRecordsOuter = 0;
            if (oidOuter == null)
            {
                nrRecordsOuter = tiOuter.Header.numberOfRecords;
                outCheckForDeleted = true;
            }
            else
            {
                nrRecordsOuter = oidOuter.Count;
            }

            var nrRecordsInner = 0;
            if (oidInner == null)
            {
                nrRecordsInner = tiInner.Header.numberOfRecords;
                innCheckForDeleted = true;
            }
            else
            {
                nrRecordsInner = oidInner.Count;
            }
#if UNITY3D
            Dictionary<int, object> outerDict =
 new Dictionary<int, object>(new sqoDB.Utilities.EqualityComparer<int>());
			Dictionary<int, object> innerDict =
 new Dictionary<int, object>(new sqoDB.Utilities.EqualityComparer<int>());
#else
            var outerDict = new Dictionary<int, object>();
            var innerDict = new Dictionary<int, object>();
#endif
            for (var i = 0; i < nrRecordsOuter; i++)
            {
                var oidOut = oidOuter == null ? i + 1 : oidOuter[i];
                if (outCheckForDeleted)
                    if (serializerOuter.IsObjectDeleted(oidOut, tiOuter))
                        continue;
                if (string.Compare(criteriaOuter, "OID") == 0)
                {
                    outerDict.Add(oidOut, oidOut);
                }
                else
                {
                    var val = serializerOuter.ReadFieldValue(tiOuter, oidOut, criteriaOuter);
                    if (val != null) //added when nullable types was added
                        outerDict.Add(oidOut, val);
                }
            }

            for (var j = 0; j < nrRecordsInner; j++)
            {
                var oidInn = oidInner == null ? j + 1 : oidInner[j];
                if (innCheckForDeleted)
                    if (serializerInner.IsObjectDeleted(oidInn, tiInner))
                        continue;
                if (string.Compare(criteriaInner, "OID") == 0)
                {
                    innerDict.Add(oidInn, oidInn);
                }
                else
                {
                    var valInner = serializerInner.ReadFieldValue(tiInner, oidInn, criteriaInner);
                    if (valInner != null) //added when nullable types was added
                        innerDict.Add(oidInn, valInner);
                }
            }

            foreach (var outerOid in outerDict.Keys)
            {
                var val = outerDict[outerOid];
                foreach (var innerOid in innerDict.Keys)
                {
                    var valInner = innerDict[innerOid];
                    if (val.Equals(valInner))
                    {
                        var kv = new KeyValuePair<int, int>(outerOid, innerOid);

                        oids.Add(kv);
                    }
                }
            }


            return oids;
        }
#if ASYNC
        internal async Task<List<KeyValuePair<int, int>>> LoadJoinAsync(SqoTypeInfo tiOuter, string criteriaOuter,
            List<int> oidOuter, SqoTypeInfo tiInner, string criteriaInner, List<int> oidInner)
        {
            var oids = new List<KeyValuePair<int, int>>();
            var serializerOuter = SerializerFactory.GetSerializer(path, GetFileByType(tiOuter), useElevatedTrust);
            var serializerInner = SerializerFactory.GetSerializer(path, GetFileByType(tiInner), useElevatedTrust);
            var outCheckForDeleted = false;
            var innCheckForDeleted = false;

            var nrRecordsOuter = 0;
            if (oidOuter == null)
            {
                nrRecordsOuter = tiOuter.Header.numberOfRecords;
                outCheckForDeleted = true;
            }
            else
            {
                nrRecordsOuter = oidOuter.Count;
            }

            var nrRecordsInner = 0;
            if (oidInner == null)
            {
                nrRecordsInner = tiInner.Header.numberOfRecords;
                innCheckForDeleted = true;
            }
            else
            {
                nrRecordsInner = oidInner.Count;
            }
#if UNITY3D
            Dictionary<int, object> outerDict =
 new Dictionary<int, object>(new sqoDB.Utilities.EqualityComparer<int>());
			Dictionary<int, object> innerDict =
 new Dictionary<int, object>(new sqoDB.Utilities.EqualityComparer<int>());
#else
            var outerDict = new Dictionary<int, object>();
            var innerDict = new Dictionary<int, object>();
#endif
            for (var i = 0; i < nrRecordsOuter; i++)
            {
                var oidOut = oidOuter == null ? i + 1 : oidOuter[i];
                if (outCheckForDeleted)
                    if (await serializerOuter.IsObjectDeletedAsync(oidOut, tiOuter).ConfigureAwait(false))
                        continue;
                if (string.Compare(criteriaOuter, "OID") == 0)
                {
                    outerDict.Add(oidOut, oidOut);
                }
                else
                {
                    var val = await serializerOuter.ReadFieldValueAsync(tiOuter, oidOut, criteriaOuter)
                        .ConfigureAwait(false);
                    if (val != null) //added when nullable types was added
                        outerDict.Add(oidOut, val);
                }
            }

            for (var j = 0; j < nrRecordsInner; j++)
            {
                var oidInn = oidInner == null ? j + 1 : oidInner[j];
                if (innCheckForDeleted)
                    if (await serializerInner.IsObjectDeletedAsync(oidInn, tiInner).ConfigureAwait(false))
                        continue;
                if (string.Compare(criteriaInner, "OID") == 0)
                {
                    innerDict.Add(oidInn, oidInn);
                }
                else
                {
                    var valInner = await serializerInner.ReadFieldValueAsync(tiInner, oidInn, criteriaInner)
                        .ConfigureAwait(false);
                    if (valInner != null) //added when nullable types was added
                        innerDict.Add(oidInn, valInner);
                }
            }

            foreach (var outerOid in outerDict.Keys)
            {
                var val = outerDict[outerOid];
                foreach (var innerOid in innerDict.Keys)
                {
                    var valInner = innerDict[innerOid];
                    if (val.Equals(valInner))
                    {
                        var kv = new KeyValuePair<int, int>(outerOid, innerOid);

                        oids.Add(kv);
                    }
                }
            }


            return oids;
        }

#endif
        internal object LoadValue(int oid, string fieldName, SqoTypeInfo ti)
        {
            if (fieldName == "OID") return oid;
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObject += serializer_NeedReadComplexObject;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;
            return serializer.ReadFieldValue(ti, oid, fieldName, rawSerializer);
        }
#if ASYNC
        internal async Task<object> LoadValueAsync(int oid, string fieldName, SqoTypeInfo ti)
        {
            if (fieldName == "OID") return oid;
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObjectAsync += serializer_NeedReadComplexObjectAsync;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            return await serializer.ReadFieldValueAsync(ti, oid, fieldName, rawSerializer).ConfigureAwait(false);
        }
#endif
        internal List<int> LoadAllOIDs(SqoTypeInfo ti)
        {
            var oids = new List<int>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            var nrRecords = ti.Header.numberOfRecords;
            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (serializer.IsObjectDeleted(oid, ti)) continue;
                oids.Add(oid);
            }

            return oids;
        }
#if ASYNC
        internal async Task<List<int>> LoadAllOIDsAsync(SqoTypeInfo ti)
        {
            var oids = new List<int>();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            var nrRecords = ti.Header.numberOfRecords;
            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false)) continue;
                oids.Add(oid);
            }

            return oids;
        }
#endif
        internal List<int> LoadAllOIDs(string typeName)
        {
            var ti = GetSqoTypeInfo(typeName);
            if (ti == null) return null;
            return LoadAllOIDs(ti);
        }
#if ASYNC
        internal async Task<List<int>> LoadAllOIDsAsync(string typeName)
        {
            var ti = await GetSqoTypeInfoAsync(typeName).ConfigureAwait(false);
            if (ti == null) return null;
            return await LoadAllOIDsAsync(ti).ConfigureAwait(false);
        }
#endif
        internal object LoadObjectByOID(SqoTypeInfo ti, int oid, List<string> includes)
        {
            if (includePropertiesCache == null) includePropertiesCache = new List<ATuple<Type, string>>();
            foreach (var path in includes)
            {
                var arrayPath = path.Split('.');

                PropertyInfo property;
                var type = ti.Type;
                foreach (var include in arrayPath)
                {
                    if ((property = type.GetProperty(include)) == null)
                    {
                        if (typeof(IList).IsAssignableFrom(type))
                        {
                            var elementType = type.GetElementType();
                            if (elementType == null) elementType = type.GetProperty("Item").PropertyType;
                            type = elementType;
                            if ((property = type.GetProperty(include)) == null)
                                throw new SiaqodbException("Property:" + include + " does not belong to Type:" +
                                                           type.FullName);
                        }
                        else
                        {
                            throw new SiaqodbException("Property:" + include + " does not belong to Type:" +
                                                       type.FullName);
                        }
                    }

                    var backingField = ExternalMetaHelper.GetBackingField(property);
                    if (!ExistsInIncludesCache(type, backingField))
                        includePropertiesCache.Add(new ATuple<Type, string>(type, backingField));
                    type = property.PropertyType;
                }
            }

            var obj = LoadObjectByOID(ti, oid);
            includePropertiesCache.Clear();

            return obj;
        }
#if ASYNC
        internal async Task<object> LoadObjectByOIDAsync(SqoTypeInfo ti, int oid, List<string> includes)
        {
            if (includePropertiesCache == null) includePropertiesCache = new List<ATuple<Type, string>>();
            foreach (var path in includes)
            {
                var arrayPath = path.Split('.');

                PropertyInfo property;
                var type = ti.Type;
                foreach (var include in arrayPath)
                {
                    if ((property = type.GetProperty(include)) == null)
                    {
                        if (typeof(IList).IsAssignableFrom(type))
                        {
                            var elementType = type.GetElementType();
                            if (elementType == null) elementType = type.GetProperty("Item").PropertyType;
                            type = elementType;
                            if ((property = type.GetProperty(include)) == null)
                                throw new SiaqodbException("Property:" + include + " does not belong to Type:" +
                                                           type.FullName);
                        }
                        else
                        {
                            throw new SiaqodbException("Property:" + include + " does not belong to Type:" +
                                                       type.FullName);
                        }
                    }

                    var backingField = ExternalMetaHelper.GetBackingField(property);
                    if (!ExistsInIncludesCache(type, backingField))
                        includePropertiesCache.Add(new ATuple<Type, string>(type, backingField));
                    type = property.PropertyType;
                }
            }

            var obj = await LoadObjectByOIDAsync(ti, oid).ConfigureAwait(false);
            includePropertiesCache.Clear();

            return obj;
        }
#endif
        internal object LoadObjectByOID(SqoTypeInfo ti, int oid)
        {
            return LoadObjectByOID(ti, oid, true);
        }
#if ASYNC
        internal async Task<object> LoadObjectByOIDAsync(SqoTypeInfo ti, int oid)
        {
            return await LoadObjectByOIDAsync(ti, oid, true).ConfigureAwait(false);
        }
#endif

        internal object LoadObjectByOID(SqoTypeInfo ti, int oid, bool clearCache)
        {
            object currentObj = null;

            if (SiaqodbConfigurator.RaiseLoadEvents)
            {
                var args = new LoadingObjectEventArgs(oid, ti.Type);
                OnLoadingObject(args);
                if (args.Cancel)
                    return null;
                if (args.Replace != null) return args.Replace;
            }

            currentObj = Activator.CreateInstance(ti.Type);
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObject += serializer_NeedReadComplexObject;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            if (clearCache) circularRefCache.Clear();
            circularRefCache.Add(oid, ti, currentObj);
            try
            {
                serializer.ReadObject(currentObj, ti, oid, rawSerializer);
            }
            catch (ArgumentException ex)
            {
                SiaqodbConfigurator.LogMessage("Object with OID:" + oid + " seems to be corrupted!",
                    VerboseLevel.Error);

                if (SiaqodbUtil.IsRepairMode)
                {
                    SiaqodbConfigurator.LogMessage("Object with OID:" + oid + " is deleted", VerboseLevel.Warn);

                    DeleteObjectByOID(oid, ti);
                    return null;
                }

                throw ex;
            }

            metaCache.SetOIDToObject(currentObj, oid, ti);

            if (SiaqodbConfigurator.RaiseLoadEvents) OnLoadedObject(oid, currentObj);
            return currentObj;
        }
#if ASYNC
        internal async Task<object> LoadObjectByOIDAsync(SqoTypeInfo ti, int oid, bool clearCache)
        {
            object currentObj = null;

            if (SiaqodbConfigurator.RaiseLoadEvents)
            {
                var args = new LoadingObjectEventArgs(oid, ti.Type);
                OnLoadingObject(args);
                if (args.Cancel)
                    return null;
                if (args.Replace != null) return args.Replace;
            }

            currentObj = Activator.CreateInstance(ti.Type);
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedReadComplexObjectAsync += serializer_NeedReadComplexObjectAsync;
            serializer.NeedCacheDocument += serializer_NeedCacheDocument;

            if (clearCache) circularRefCache.Clear();
            circularRefCache.Add(oid, ti, currentObj);
            var exTh = false;
            try
            {
                await serializer.ReadObjectAsync(currentObj, ti, oid, rawSerializer).ConfigureAwait(false);
            }
            catch (ArgumentException ex)
            {
                SiaqodbConfigurator.LogMessage("Object with OID:" + oid + " seems to be corrupted!",
                    VerboseLevel.Error);

                if (SiaqodbUtil.IsRepairMode)
                {
                    SiaqodbConfigurator.LogMessage("Object with OID:" + oid + " is deleted", VerboseLevel.Warn);

                    exTh = true;
                }
                else
                {
                    throw ex;
                }
            }

            if (exTh)
            {
                await DeleteObjectByOIDAsync(oid, ti).ConfigureAwait(false);
                return null;
            }

            metaCache.SetOIDToObject(currentObj, oid, ti);

            if (SiaqodbConfigurator.RaiseLoadEvents) OnLoadedObject(oid, currentObj);
            return currentObj;
        }

#endif
        internal T LoadObjectByOID<T>(SqoTypeInfo ti, int oid)
        {
            return LoadObjectByOID<T>(ti, oid, true);
        }
#if ASYNC
        internal async Task<T> LoadObjectByOIDAsync<T>(SqoTypeInfo ti, int oid)
        {
            return await LoadObjectByOIDAsync<T>(ti, oid, true).ConfigureAwait(false);
        }
#endif
        internal T LoadObjectByOID<T>(SqoTypeInfo ti, int oid, bool clearCache)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            if (oid > 0 && oid <= ti.Header.numberOfRecords && !serializer.IsObjectDeleted(oid, ti))
                return (T)LoadObjectByOID(ti, oid, clearCache);
            return default;
        }
#if ASYNC
        internal async Task<T> LoadObjectByOIDAsync<T>(SqoTypeInfo ti, int oid, bool clearCache)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            if (oid > 0 && oid <= ti.Header.numberOfRecords &&
                !await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false))
                return (T)await LoadObjectByOIDAsync(ti, oid, clearCache).ConfigureAwait(false);
            return default;
        }
#endif
        internal int Count(SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            var nrRecords = ti.Header.numberOfRecords;
            var rangeSize = Convert.ToInt32(SiaqodbConfigurator.BufferingChunkPercent * nrRecords / 100);
            if (rangeSize < 1) rangeSize = 1;

            var count = 0;
            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (i % rangeSize == 0)
                {
                    var oidEnd = i + rangeSize <= nrRecords ? i + rangeSize : nrRecords;
                    serializer.PreLoadBytes(oid, oidEnd, ti);
                }

                if (serializer.IsObjectDeleted(oid, ti)) continue;
                count++;
            }

            serializer.ResetPreload();
            return count;
        }
#if ASYNC
        internal async Task<int> CountAsync(SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            var nrRecords = ti.Header.numberOfRecords;
            var rangeSize = Convert.ToInt32(SiaqodbConfigurator.BufferingChunkPercent * nrRecords / 100);
            if (rangeSize < 1) rangeSize = 1;

            var count = 0;
            for (var i = 0; i < nrRecords; i++)
            {
                var oid = i + 1;
                if (i % rangeSize == 0)
                {
                    var oidEnd = i + rangeSize <= nrRecords ? i + rangeSize : nrRecords;
                    await serializer.PreLoadBytesAsync(oid, oidEnd, ti).ConfigureAwait(false);
                }

                if (await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false)) continue;
                count++;
            }

            serializer.ResetPreload();
            return count;
        }
#endif
        internal ATuple<int, int> GetArrayMetaOfField(SqoTypeInfo ti, int oid, FieldSqoInfo fi)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return serializer.GetArrayMetaOfField(ti, oid, fi);
        }
#if ASYNC
        internal async Task<ATuple<int, int>> GetArrayMetaOfFieldAsync(SqoTypeInfo ti, int oid, FieldSqoInfo fi)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return await serializer.GetArrayMetaOfFieldAsync(ti, oid, fi).ConfigureAwait(false);
        }
#endif
        private bool ExistsInIncludesCache(Type type, string fieldName)
        {
            if (includePropertiesCache == null) return false;
            foreach (var tuple in includePropertiesCache)
                if (tuple.Name == type && tuple.Value == fieldName)
                    return true;
            return false;
        }

        internal KeyValuePair<int, int> LoadOIDAndTID(int oid, FieldSqoInfo fi, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return serializer.ReadOIDAndTID(ti, oid, fi);
        }
#if ASYNC
        internal async Task<KeyValuePair<int, int>> LoadOIDAndTIDAsync(int oid, FieldSqoInfo fi, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return await serializer.ReadOIDAndTIDAsync(ti, oid, fi).ConfigureAwait(false);
        }
#endif
        internal List<KeyValuePair<int, int>> LoadComplexArray(int oid, FieldSqoInfo fi, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return serializer.ReadComplexArrayOids(oid, fi, ti, rawSerializer);
        }
#if ASYNC
        internal async Task<List<KeyValuePair<int, int>>> LoadComplexArrayAsync(int oid, FieldSqoInfo fi,
            SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return await serializer.ReadComplexArrayOidsAsync(oid, fi, ti, rawSerializer).ConfigureAwait(false);
        }
#endif
        internal int LoadComplexArrayTID(int oid, FieldSqoInfo fi, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return serializer.ReadFirstTID(oid, fi, ti, rawSerializer);
        }
#if ASYNC
        internal async Task<int> LoadComplexArrayTIDAsync(int oid, FieldSqoInfo fi, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return await serializer.ReadFirstTIDAsync(oid, fi, ti, rawSerializer).ConfigureAwait(false);
        }
#endif
        internal bool IsObjectDeleted(int oid, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            if (oid > 0 && oid <= ti.Header.numberOfRecords && !serializer.IsObjectDeleted(oid, ti)) return false;
            return true;
        }
#if ASYNC
        internal async Task<bool> IsObjectDeletedAsync(int oid, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            if (oid > 0 && oid <= ti.Header.numberOfRecords &&
                !await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false)) return false;
            return true;
        }
#endif

        internal List<int> GetUsedRawdataInfoOIDs(SqoTypeInfo ti)
        {
            var existingRawdataInfoOIDs = new List<int>();
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
                    if (serializer.IsObjectDeleted(oid, ti)) continue;
                    foreach (var ai in existingDynamicFields)
                    {
                        var arrayInfo = GetArrayMetaOfField(ti, oid, ai);
                        if (arrayInfo.Name > 0) existingRawdataInfoOIDs.Add(arrayInfo.Name);
                    }
                }

            return existingRawdataInfoOIDs;
        }
#if ASYNC
        internal async Task<List<int>> GetUsedRawdataInfoOIDsAsync(SqoTypeInfo ti)
        {
            var existingRawdataInfoOIDs = new List<int>();
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
                    if (await serializer.IsObjectDeletedAsync(oid, ti).ConfigureAwait(false)) continue;
                    foreach (var ai in existingDynamicFields)
                    {
                        var arrayInfo = await GetArrayMetaOfFieldAsync(ti, oid, ai).ConfigureAwait(false);
                        if (arrayInfo.Name > 0) existingRawdataInfoOIDs.Add(arrayInfo.Name);
                    }
                }

            return existingRawdataInfoOIDs;
        }
#endif
        internal List<int> LoadOidsByField(SqoTypeInfo ti, string fieldName, object obj)
        {
            var objInfo = MetaExtractor.GetObjectInfo(obj, ti, metaCache);
            var fi = MetaHelper.FindField(ti.Fields, fieldName);
            if (fi == null)
                throw new SiaqodbException("Field:" + fieldName + " was not found as member of Type:" + ti.TypeName);
            var w = new Where(fi.Name, OperationType.Equal, objInfo.AtInfo[fi]);

            return LoadFilteredOids(w, ti);
        }

        internal Dictionary<int, ATuple<int, FieldSqoInfo>> GetUsedRawdataInfoOIDsAndFieldInfos(SqoTypeInfo ti)
        {
            var existingRawdataInfoOIDs = new Dictionary<int, ATuple<int, FieldSqoInfo>>();
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
                    if (serializer.IsObjectDeleted(oid, ti)) continue;
                    foreach (var ai in existingDynamicFields)
                    {
                        var arrayInfo = GetArrayMetaOfField(ti, oid, ai);
                        if (arrayInfo.Name > 0)
                            existingRawdataInfoOIDs.Add(arrayInfo.Name, new ATuple<int, FieldSqoInfo>(oid, ai));
                    }
                }

            return existingRawdataInfoOIDs;
        }
    }
}