using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using sqoDB.Exceptions;
using sqoDB.Meta;
using sqoDB.Utilities;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Core
{
    internal partial class ObjectSerializer
    {
        [Obfuscation(Exclude = true)] private EventHandler<ComplexObjectEventArgs> needSaveComplexObject;

        internal void SaveOID(int oid, SqoTypeInfo ti)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);
            var oidBuff = ByteConverter.IntToByteArray(oid);
            file.Write(position, oidBuff);
        }
#if ASYNC
        internal async Task SaveOIDAsync(int oid, SqoTypeInfo ti)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);
            var oidBuff = ByteConverter.IntToByteArray(oid);
            await file.WriteAsync(position, oidBuff).ConfigureAwait(false);
        }
#endif
        public void SerializeObject(ObjectInfo oi, RawdataSerializer rawSerializer)
        {
            if (oi.Oid > oi.SqoTypeInfo.Header.numberOfRecords) oi.Oid = 0;
            if (oi.Oid == 0)
            {
                oi.Oid = GetNextOID(oi.SqoTypeInfo);
                //SaveOID(oi.SqoTypeInfo, oi.Oid);
                oi.SqoTypeInfo.Header.numberOfRecords++; //it is needed here if exists a nested object of same type

                oi.Inserted = true;
            }
            else if (oi.Oid < 0)
            {
                throw new SiaqodbException("Object is already deleted from database");
            }

            var position = MetaHelper.GetSeekPosition(oi.SqoTypeInfo, oi.Oid);

            var buffer = GetObjectBytes(oi, rawSerializer);

            file.Write(position, buffer);

            if (oi.Inserted) SaveNrRecords(oi.SqoTypeInfo, oi.SqoTypeInfo.Header.numberOfRecords);
        }
#if ASYNC
        public async Task SerializeObjectAsync(ObjectInfo oi, RawdataSerializer rawSerializer)
        {
            if (oi.Oid > oi.SqoTypeInfo.Header.numberOfRecords) oi.Oid = 0;
            if (oi.Oid == 0)
            {
                oi.Oid = GetNextOID(oi.SqoTypeInfo);
                //SaveOID(oi.SqoTypeInfo, oi.Oid);
                oi.SqoTypeInfo.Header.numberOfRecords++; //it is needed here if exists a nested object of same type

                oi.Inserted = true;
            }
            else if (oi.Oid < 0)
            {
                throw new SiaqodbException("Object is already deleted from database");
            }

            var position = MetaHelper.GetSeekPosition(oi.SqoTypeInfo, oi.Oid);

            var buffer = await GetObjectBytesAsync(oi, rawSerializer).ConfigureAwait(false);

            await file.WriteAsync(position, buffer).ConfigureAwait(false);

            if (oi.Inserted)
                await SaveNrRecordsAsync(oi.SqoTypeInfo, oi.SqoTypeInfo.Header.numberOfRecords).ConfigureAwait(false);
        }
#endif
        public void SerializeObject(byte[] objectData, int oid, SqoTypeInfo ti, bool insert)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);

            file.Write(position, objectData);

            if (insert) SaveNrRecords(ti, ti.Header.numberOfRecords + 1);
        }
#if ASYNC
        public async Task SerializeObjectAsync(byte[] objectData, int oid, SqoTypeInfo ti, bool insert)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);

            await file.WriteAsync(position, objectData).ConfigureAwait(false);

            if (insert) await SaveNrRecordsAsync(ti, ti.Header.numberOfRecords + 1).ConfigureAwait(false);
        }
#endif
        public int SerializeObjectWithNewOID(byte[] objectData, SqoTypeInfo ti)
        {
            var oid = GetNextOID(ti);
            var oidBuff = ByteConverter.IntToByteArray(oid);
            Array.Copy(oidBuff, 0, objectData, 0, oidBuff.Length);

            var position = MetaHelper.GetSeekPosition(ti, oid);

            file.Write(position, objectData);

            SaveNrRecords(ti, ti.Header.numberOfRecords + 1);
            return oid;
        }
#if ASYNC
        public async Task<int> SerializeObjectWithNewOIDAsync(byte[] objectData, SqoTypeInfo ti)
        {
            var oid = GetNextOID(ti);
            var oidBuff = ByteConverter.IntToByteArray(oid);
            Array.Copy(oidBuff, 0, objectData, 0, oidBuff.Length);

            var position = MetaHelper.GetSeekPosition(ti, oid);

            await file.WriteAsync(position, objectData).ConfigureAwait(false);

            await SaveNrRecordsAsync(ti, ti.Header.numberOfRecords + 1).ConfigureAwait(false);
            return oid;
        }
#endif
        internal byte[] GetObjectBytes(ObjectInfo oi, RawdataSerializer rawSerializer)
        {
            var oidBuff = ByteConverter.IntToByteArray(oi.Oid);
            var buffer = new byte[oi.SqoTypeInfo.Header.lengthOfRecord];

            var curentIndex = 0;
            Array.Copy(oidBuff, 0, buffer, curentIndex, oidBuff.Length);
            curentIndex += oidBuff.Length;

            var oidToParentSet = false;

            foreach (var ai in oi.AtInfo.Keys)
            {
                byte[] by = null;
                if (ai.AttributeTypeId == MetaExtractor.complexID ||
                    ai.AttributeTypeId == MetaExtractor.ArrayTypeIDExtra + MetaExtractor.complexID ||
                    ai.AttributeTypeId == MetaExtractor.documentID)
                    //to be able to cache for circular reference, we need to asign OID to it
                    if (!oidToParentSet)
                    {
                        //just set OID to parentObject, do not save anything
                        var args = new ComplexObjectEventArgs(true, oi);
                        OnNeedSaveComplexObject(args);

                        oidToParentSet = true;
                    }

                var parentOID = -1;
                if (!oi.Inserted) parentOID = oi.Oid;
                var byteTransformer =
                    ByteTransformerFactory.GetByteTransformer(this, rawSerializer, ai, oi.SqoTypeInfo, parentOID);

                by = byteTransformer.GetBytes(oi.AtInfo[ai]);

                Array.Copy(by, 0, buffer, curentIndex, by.Length);
                curentIndex += by.Length;
            }

            return buffer;
        }
#if ASYNC
        internal async Task<byte[]> GetObjectBytesAsync(ObjectInfo oi, RawdataSerializer rawSerializer)
        {
            var oidBuff = ByteConverter.IntToByteArray(oi.Oid);
            var buffer = new byte[oi.SqoTypeInfo.Header.lengthOfRecord];

            var curentIndex = 0;
            Array.Copy(oidBuff, 0, buffer, curentIndex, oidBuff.Length);
            curentIndex += oidBuff.Length;

            var oidToParentSet = false;

            foreach (var ai in oi.AtInfo.Keys)
            {
                byte[] by = null;
                if (ai.AttributeTypeId == MetaExtractor.complexID ||
                    ai.AttributeTypeId == MetaExtractor.ArrayTypeIDExtra + MetaExtractor.complexID ||
                    ai.AttributeTypeId == MetaExtractor.documentID)
                    //to be able to cache for circular reference, we need to asign OID to it
                    if (!oidToParentSet)
                    {
                        //just set OID to parentObject, do not save anything
                        var args = new ComplexObjectEventArgs(true, oi);
                        await OnNeedSaveComplexObjectAsync(args).ConfigureAwait(false);

                        oidToParentSet = true;
                    }

                var parentOID = -1;
                if (!oi.Inserted) parentOID = oi.Oid;
                var byteTransformer =
                    ByteTransformerFactory.GetByteTransformer(this, rawSerializer, ai, oi.SqoTypeInfo, parentOID);

                by = await byteTransformer.GetBytesAsync(oi.AtInfo[ai]).ConfigureAwait(false);

                Array.Copy(by, 0, buffer, curentIndex, by.Length);
                curentIndex += by.Length;
            }

            return buffer;
        }

#endif
        public byte[] GetComplexObjectBytes(object obj, bool returnOnlyOID_TID)
        {
            var args = new ComplexObjectEventArgs(obj, returnOnlyOID_TID);
            OnNeedSaveComplexObject(args);
            var by = new byte[MetaExtractor.GetAbsoluteSizeOfField(MetaExtractor.complexID)];
            var complexOID = ByteConverter.IntToByteArray(args.SavedOID);
            var complexTID = ByteConverter.IntToByteArray(args.TID);
            Array.Copy(complexOID, 0, by, 0, complexOID.Length);
            Array.Copy(complexTID, 0, by, 4, complexTID.Length);
            return by;
        }
#if ASYNC
        public async Task<byte[]> GetComplexObjectBytesAsync(object obj, bool returnOnlyOID_TID)
        {
            var args = new ComplexObjectEventArgs(obj, returnOnlyOID_TID);
            await OnNeedSaveComplexObjectAsync(args).ConfigureAwait(false);
            var by = new byte[MetaExtractor.GetAbsoluteSizeOfField(MetaExtractor.complexID)];
            var complexOID = ByteConverter.IntToByteArray(args.SavedOID);
            var complexTID = ByteConverter.IntToByteArray(args.TID);
            Array.Copy(complexOID, 0, by, 0, complexOID.Length);
            Array.Copy(complexTID, 0, by, 4, complexTID.Length);
            return by;
        }
#endif
        public byte[] GetComplexObjectBytes(object obj)
        {
            return GetComplexObjectBytes(obj, false);
        }
#if ASYNC
        public async Task<byte[]> GetComplexObjectBytesAsync(object obj)
        {
            return await GetComplexObjectBytesAsync(obj, false).ConfigureAwait(false);
        }
#endif
        private int GetNextOID(SqoTypeInfo typeInfo)
        {
            return typeInfo.Header.numberOfRecords + 1;
        }

        public void SaveNrRecords(SqoTypeInfo ti, int nrRecords)
        {
            ti.Header.numberOfRecords = nrRecords;
            var nrRecodsBuf = ByteConverter.IntToByteArray(ti.Header.numberOfRecords);
            file.Write(ti.Header.typeNameSize + 16, nrRecodsBuf);
        }
#if ASYNC
        public async Task SaveNrRecordsAsync(SqoTypeInfo ti, int nrRecords)
        {
            ti.Header.numberOfRecords = nrRecords;
            var nrRecodsBuf = ByteConverter.IntToByteArray(ti.Header.numberOfRecords);
            await file.WriteAsync(ti.Header.typeNameSize + 16, nrRecodsBuf).ConfigureAwait(false);
        }
#endif
        internal void MarkObjectAsDelete(int oid, SqoTypeInfo ti)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);
            var deletedOID = -1 * oid;
            var deletedOidBuff = ByteConverter.IntToByteArray(deletedOID);

            file.Write(position, deletedOidBuff);
        }
#if ASYNC
        internal async Task MarkObjectAsDeleteAsync(int oid, SqoTypeInfo ti)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);
            var deletedOID = -1 * oid;
            var deletedOidBuff = ByteConverter.IntToByteArray(deletedOID);

            await file.WriteAsync(position, deletedOidBuff).ConfigureAwait(false);
        }
#endif
        internal void RollbackDeleteObject(int oid, SqoTypeInfo ti)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);
            var oidBuff = ByteConverter.IntToByteArray(oid);
            lock (file)
            {
                file.Write(position, oidBuff);
            }
        }
#if ASYNC
        internal async Task RollbackDeleteObjectAsync(int oid, SqoTypeInfo ti)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);
            var oidBuff = ByteConverter.IntToByteArray(oid);
            await file.WriteAsync(position, oidBuff).ConfigureAwait(false);
        }

#endif
        internal bool SaveFieldValue(int oid, string field, SqoTypeInfo ti, object value,
            RawdataSerializer rawSerializer)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);
            var recordLength = ti.Header.lengthOfRecord;
            var ai = FindField(ti.Fields, field);
            if (ai == null)
                throw new SiaqodbException("Field:" + field +
                                           " not exists in the Type Definition, if you use a Property you have to use UseVariable Attribute");
            if (value != null && ai.AttributeType != value.GetType())
                try
                {
                    var valConvert = Convertor.ChangeType(value, ai.AttributeType);

                    value = valConvert;
                }
                catch
                {
                    var msg = "Type of value should be:" + ai.AttributeType;
                    SiaqodbConfigurator.LogMessage(msg, VerboseLevel.Error);
                    throw new SiaqodbException(msg);
                }

            byte[] by = null;

            var byteTransformer = ByteTransformerFactory.GetByteTransformer(this, rawSerializer, ai, ti, oid);
            by = byteTransformer.GetBytes(value);

            file.Write(position + ai.Header.PositionInRecord, by);

            return true;
        }

#if ASYNC
        internal async Task<bool> SaveFieldValueAsync(int oid, string field, SqoTypeInfo ti, object value,
            RawdataSerializer rawSerializer)
        {
            var position = MetaHelper.GetSeekPosition(ti, oid);
            var recordLength = ti.Header.lengthOfRecord;
            var ai = FindField(ti.Fields, field);
            if (ai == null)
                throw new SiaqodbException("Field:" + field +
                                           " not exists in the Type Definition, if you use a Property you have to use UseVariable Attribute");
            if (value != null && ai.AttributeType != value.GetType())
                try
                {
                    var valConvert = Convertor.ChangeType(value, ai.AttributeType);

                    value = valConvert;
                }
                catch
                {
                    var msg = "Type of value should be:" + ai.AttributeType;
                    SiaqodbConfigurator.LogMessage(msg, VerboseLevel.Error);
                    throw new SiaqodbException(msg);
                }

            byte[] by = null;

            var byteTransformer = ByteTransformerFactory.GetByteTransformer(this, rawSerializer, ai, ti, oid);
            by = await byteTransformer.GetBytesAsync(value).ConfigureAwait(false);

            await file.WriteAsync(position + ai.Header.PositionInRecord, by).ConfigureAwait(false);

            return true;
        }
#endif

        internal int InsertEmptyObject(SqoTypeInfo tinf)
        {
            var oid = GetNextOID(tinf);
            var oidBuff = ByteConverter.IntToByteArray(oid);
            var position = MetaHelper.GetSeekPosition(tinf, oid);
            file.Write(position, oidBuff);
            SaveNrRecords(tinf, tinf.Header.numberOfRecords + 1);
            return oid;
        }

        internal void SaveObjectTable(SqoTypeInfo actualTypeinfo, SqoTypeInfo oldSqoTypeInfo, ObjectTable table,
            RawdataSerializer rawSerializer)
        {
            var joinedFields = JoinFieldsSqoInfo(actualTypeinfo, oldSqoTypeInfo);

            foreach (var row in table.Rows)
            {
                var oid = (int)row["OID"];
                if (oid < 0) //deleted
                {
                    MarkObjectAsDelete(-oid, actualTypeinfo);
                    continue;
                }

                var oidBuff = ByteConverter.IntToByteArray(oid);
                var buffer = new byte[actualTypeinfo.Header.lengthOfRecord];

                var curentIndex = 0;
                Array.Copy(oidBuff, 0, buffer, curentIndex, oidBuff.Length);
                curentIndex += oidBuff.Length;
                foreach (var ai in actualTypeinfo.Fields)
                {
                    byte[] by = null;


                    object fieldVal = null;
                    var existed = false;
                    if (table.Columns.ContainsKey(ai.Name))
                    {
                        fieldVal = row[ai.Name];
                        existed = true;
                    }
                    else
                    {
                        if (ai.AttributeTypeId == MetaExtractor.complexID ||
                            ai.AttributeTypeId == MetaExtractor.documentID)
                            fieldVal = null;
                        else if (typeof(string) == ai.AttributeType)
                            fieldVal = string.Empty;
                        else if (ai.AttributeType.IsArray)
                            fieldVal = Array.CreateInstance(ai.AttributeType.GetElementType(), 0);
                        else
                            fieldVal = Activator.CreateInstance(ai.AttributeType);
                    }

                    if (joinedFields[ai] != null) //existed in old Type
                        if (ai.AttributeTypeId != joinedFields[ai].AttributeTypeId)
                        {
                            if (typeof(IList).IsAssignableFrom(ai.AttributeType) ||
                                ai.AttributeTypeId == MetaExtractor.dictionaryID ||
                                joinedFields[ai].AttributeTypeId == MetaExtractor.dictionaryID)
                                throw new TypeChangedException("Change array or dictionary type it is not supported");
                            fieldVal = Convertor.ChangeType(fieldVal, ai.AttributeType);
                        }

                    if (ai.AttributeTypeId == MetaExtractor.complexID || ai.AttributeTypeId == MetaExtractor.documentID)
                    {
                        if (existed)
                            by = (byte[])fieldVal;
                        else
                            by = GetComplexObjectBytes(fieldVal);
                    }
                    else if (typeof(IList).IsAssignableFrom(ai.AttributeType)) //array
                    {
                        if (existed)
                            by = (byte[])fieldVal;
                        else
                            by = rawSerializer.SerializeArray(fieldVal, ai.AttributeType, ai.Header.Length,
                                ai.Header.RealLength, actualTypeinfo.Header.version, null, this, ai.IsText);
                    }
                    else if (ai.IsText)
                    {
                        if (existed)
                        {
                            var oldAi = joinedFields[ai];
                            if (oldAi != null && oldAi.IsText)
                                by = (byte[])fieldVal;
                            else
                                by = rawSerializer.SerializeArray(fieldVal, ai.AttributeType, ai.Header.Length,
                                    ai.Header.RealLength, actualTypeinfo.Header.version, null, this, ai.IsText);
                        }
                        else
                        {
                            by = rawSerializer.SerializeArray(fieldVal, ai.AttributeType, ai.Header.Length,
                                ai.Header.RealLength, actualTypeinfo.Header.version, null, this, ai.IsText);
                        }
                    }
                    else if (ai.AttributeTypeId == MetaExtractor.dictionaryID)
                    {
                        if (existed)
                        {
                            by = (byte[])fieldVal;
                        }
                        else
                        {
                            var byteTransformer =
                                ByteTransformerFactory.GetByteTransformer(this, rawSerializer, ai, actualTypeinfo, 0);
                            by = byteTransformer.GetBytes(fieldVal);
                        }
                    }
                    else
                    {
                        by = ByteConverter.SerializeValueType(fieldVal, ai.AttributeType, ai.Header.Length,
                            ai.Header.RealLength, actualTypeinfo.Header.version);
                    }

                    Array.Copy(by, 0, buffer, ai.Header.PositionInRecord, ai.Header.Length);
                    //curentIndex += by.Length;
                }

                var position = MetaHelper.GetSeekPosition(actualTypeinfo, oid);

                file.Write(position, buffer);
            }
        }

#if ASYNC
        internal async Task SaveObjectTableAsync(SqoTypeInfo actualTypeinfo, SqoTypeInfo oldSqoTypeInfo,
            ObjectTable table, RawdataSerializer rawSerializer)
        {
            var joinedFields = JoinFieldsSqoInfo(actualTypeinfo, oldSqoTypeInfo);

            foreach (var row in table.Rows)
            {
                var oid = (int)row["OID"];
                if (oid < 0) //deleted
                {
                    await MarkObjectAsDeleteAsync(-oid, actualTypeinfo).ConfigureAwait(false);
                    continue;
                }

                var oidBuff = ByteConverter.IntToByteArray(oid);
                var buffer = new byte[actualTypeinfo.Header.lengthOfRecord];

                var curentIndex = 0;
                Array.Copy(oidBuff, 0, buffer, curentIndex, oidBuff.Length);
                curentIndex += oidBuff.Length;
                foreach (var ai in actualTypeinfo.Fields)
                {
                    byte[] by = null;


                    object fieldVal = null;
                    var existed = false;
                    if (table.Columns.ContainsKey(ai.Name))
                    {
                        fieldVal = row[ai.Name];
                        existed = true;
                    }
                    else
                    {
                        if (ai.AttributeTypeId == MetaExtractor.complexID ||
                            ai.AttributeTypeId == MetaExtractor.documentID)
                            fieldVal = null;
                        else if (typeof(string) == ai.AttributeType)
                            fieldVal = string.Empty;
                        else if (ai.AttributeType.IsArray)
                            fieldVal = Array.CreateInstance(ai.AttributeType.GetElementType(), 0);
                        else
                            fieldVal = Activator.CreateInstance(ai.AttributeType);
                    }

                    if (joinedFields[ai] != null) //existed in old Type
                        if (ai.AttributeTypeId != joinedFields[ai].AttributeTypeId)
                        {
                            if (typeof(IList).IsAssignableFrom(ai.AttributeType) ||
                                ai.AttributeTypeId == MetaExtractor.dictionaryID ||
                                joinedFields[ai].AttributeTypeId == MetaExtractor.dictionaryID)
                                throw new TypeChangedException("Change array or dictionary type it is not supported");
                            fieldVal = Convertor.ChangeType(fieldVal, ai.AttributeType);
                        }

                    if (ai.AttributeTypeId == MetaExtractor.complexID || ai.AttributeTypeId == MetaExtractor.documentID)
                    {
                        if (existed)
                            by = (byte[])fieldVal;
                        else
                            by = await GetComplexObjectBytesAsync(fieldVal).ConfigureAwait(false);
                    }
                    else if (typeof(IList).IsAssignableFrom(ai.AttributeType)) //array
                    {
                        if (existed)
                            by = (byte[])fieldVal;
                        else
                            by = await rawSerializer.SerializeArrayAsync(fieldVal, ai.AttributeType, ai.Header.Length,
                                    ai.Header.RealLength, actualTypeinfo.Header.version, null, this, ai.IsText)
                                .ConfigureAwait(false);
                    }
                    else if (ai.IsText)
                    {
                        if (existed)
                        {
                            var oldAi = joinedFields[ai];
                            if (oldAi != null && oldAi.IsText)
                                by = (byte[])fieldVal;
                            else
                                by = await rawSerializer.SerializeArrayAsync(fieldVal, ai.AttributeType,
                                    ai.Header.Length, ai.Header.RealLength, actualTypeinfo.Header.version, null, this,
                                    ai.IsText).ConfigureAwait(false);
                        }
                        else
                        {
                            by = await rawSerializer.SerializeArrayAsync(fieldVal, ai.AttributeType, ai.Header.Length,
                                    ai.Header.RealLength, actualTypeinfo.Header.version, null, this, ai.IsText)
                                .ConfigureAwait(false);
                        }
                    }
                    else if (ai.AttributeTypeId == MetaExtractor.dictionaryID)
                    {
                        if (existed)
                        {
                            by = (byte[])fieldVal;
                        }
                        else
                        {
                            var byteTransformer =
                                ByteTransformerFactory.GetByteTransformer(this, rawSerializer, ai, actualTypeinfo, 0);
                            by = await byteTransformer.GetBytesAsync(fieldVal).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        by = ByteConverter.SerializeValueType(fieldVal, ai.AttributeType, ai.Header.Length,
                            ai.Header.RealLength, actualTypeinfo.Header.version);
                    }

                    Array.Copy(by, 0, buffer, ai.Header.PositionInRecord, ai.Header.Length);
                    //curentIndex += by.Length;
                }

                var position = MetaHelper.GetSeekPosition(actualTypeinfo, oid);

                await file.WriteAsync(position, buffer).ConfigureAwait(false);
            }
        }

#endif
        private Dictionary<FieldSqoInfo, FieldSqoInfo> JoinFieldsSqoInfo(SqoTypeInfo actualTypeinfo,
            SqoTypeInfo oldTypeinfo)
        {
            var fields = new Dictionary<FieldSqoInfo, FieldSqoInfo>();
            foreach (var fi in actualTypeinfo.Fields)
            {
                var oldFi = MetaHelper.FindField(oldTypeinfo.Fields, fi.Name);
                fields[fi] = oldFi;
            }

            return fields;
        }

        public void SaveComplexFieldContent(KeyValuePair<int, int> oid_Tid, FieldSqoInfo fi, SqoTypeInfo ti, int oid)
        {
            var by = new byte[MetaExtractor.GetAbsoluteSizeOfField(MetaExtractor.complexID)];
            var complexOID = ByteConverter.IntToByteArray(oid_Tid.Key);
            var complexTID = ByteConverter.IntToByteArray(oid_Tid.Value);
            Array.Copy(complexOID, 0, by, 0, complexOID.Length);
            Array.Copy(complexTID, 0, by, 4, complexTID.Length);

            var position = MetaHelper.GetSeekPosition(ti, oid);
            file.Write(position + fi.Header.PositionInRecord, by);
        }
#if ASYNC
        public async Task SaveComplexFieldContentAsync(KeyValuePair<int, int> oid_Tid, FieldSqoInfo fi, SqoTypeInfo ti,
            int oid)
        {
            var by = new byte[MetaExtractor.GetAbsoluteSizeOfField(MetaExtractor.complexID)];
            var complexOID = ByteConverter.IntToByteArray(oid_Tid.Key);
            var complexTID = ByteConverter.IntToByteArray(oid_Tid.Value);
            Array.Copy(complexOID, 0, by, 0, complexOID.Length);
            Array.Copy(complexTID, 0, by, 4, complexTID.Length);

            var position = MetaHelper.GetSeekPosition(ti, oid);
            await file.WriteAsync(position + fi.Header.PositionInRecord, by).ConfigureAwait(false);
        }
#endif
        internal void SaveArrayOIDFieldContent(SqoTypeInfo ti, FieldSqoInfo fi, int objectOID, int newOID)
        {
            var arrayOID = ByteConverter.IntToByteArray(newOID);
            var position = MetaHelper.GetSeekPosition(ti, objectOID);
            //an array field has size=9 (isNull(bool) + oid of array table(int)+ nrElements(int)
            //so write oid after first byte which is null/not null
            var writePosition = position + fi.Header.PositionInRecord + 1L;
            file.Write(writePosition, arrayOID);
        }

        [Obfuscation(Exclude = true)]
        public event EventHandler<ComplexObjectEventArgs> NeedSaveComplexObject
        {
            add
            {
                lock (_syncRoot)
                {
                    if (needSaveComplexObject == null) needSaveComplexObject += value;
                }
            }
            remove
            {
                lock (_syncRoot)
                {
                    needSaveComplexObject -= value;
                }
            }
        }

        protected void OnNeedSaveComplexObject(ComplexObjectEventArgs args)
        {
            EventHandler<ComplexObjectEventArgs> handler;
            lock (_syncRoot)
            {
                handler = needSaveComplexObject;
            }

            if (handler != null) handler(this, args);
        }
#if ASYNC
        protected async Task OnNeedSaveComplexObjectAsync(ComplexObjectEventArgs args)
        {
            ComplexObjectEventHandler handler;
            lock (_syncRoot)
            {
                handler = needSaveComplexObjectAsync;
            }

            if (handler != null) await handler(this, args).ConfigureAwait(false);
        }
#endif
#if ASYNC
        [Obfuscation(Exclude = true)] private ComplexObjectEventHandler needSaveComplexObjectAsync;

        [Obfuscation(Exclude = true)]
        public event ComplexObjectEventHandler NeedSaveComplexObjectAsync
        {
            add
            {
                lock (_syncRoot)
                {
                    if (needSaveComplexObjectAsync == null) needSaveComplexObjectAsync += value;
                }
            }
            remove
            {
                lock (_syncRoot)
                {
                    needSaveComplexObjectAsync -= value;
                }
            }
        }
#endif
    }
}