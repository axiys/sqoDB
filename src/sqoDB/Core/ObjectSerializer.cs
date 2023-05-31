using System;
using System.Collections.Generic;
using System.Reflection;
using sqoDB.Meta;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Core
{
    [Obfuscation(Feature = "Apply to member * when event: renaming", Exclude = true)]
    internal partial class ObjectSerializer
    {
        private readonly object _syncRoot = new object();
        private readonly string filePath;
        private ISqoFile file;

        public ObjectSerializer(string filePath, bool useElevatedTrust)
        {
            this.filePath = filePath;
            file = FileFactory.Create(filePath, false, useElevatedTrust);
        }

        public bool IsClosed => file.IsClosed;

        private byte[] SerializeTypeToBuffer(SqoTypeInfo ti)
        {
            var headerSize = ByteConverter.IntToByteArray(ti.Header.headerSize);
            var typeNameSize = ByteConverter.IntToByteArray(ti.Header.typeNameSize);
            var typeName = ByteConverter.SerializeValueType(ti.TypeName, typeof(string), ti.Header.version);
            var lastUpdate = ByteConverter.SerializeValueType(DateTime.Now, typeof(DateTime), ti.Header.version);
            var numberOfRecords = ByteConverter.IntToByteArray(ti.Header.numberOfRecords);
            var positionFirstRecord = ByteConverter.IntToByteArray(ti.Header.positionFirstRecord);
            var lengthOfRecord = ByteConverter.IntToByteArray(ti.Header.lengthOfRecord);
            var version = ByteConverter.IntToByteArray(ti.Header.version);
            var nrFields = ByteConverter.IntToByteArray(ti.Header.NrFields);
            var TID = ByteConverter.IntToByteArray(ti.Header.TID);
            var unused1 = ByteConverter.IntToByteArray(ti.Header.Unused1);
            var unused2 = ByteConverter.IntToByteArray(ti.Header.Unused2);
            var unused3 = ByteConverter.IntToByteArray(ti.Header.Unused3);

            var tArray = Combine(headerSize, typeNameSize, typeName, lastUpdate, numberOfRecords,
                positionFirstRecord, lengthOfRecord, version,
                nrFields, TID, unused1, unused2, unused3);
            var fieldIndex = 1;
            var fullArray = new byte[ti.Fields.Count + 1][];
            fullArray[0] = tArray;
            foreach (var ai in ti.Fields)
            {
                var SizeOfName = ByteConverter.IntToByteArray(ai.Header.SizeOfName);
                var aiName = ByteConverter.SerializeValueType(ai.Name, typeof(string), ti.Header.version);
                Array.Resize(ref aiName, 200);
                var attLength = ByteConverter.IntToByteArray(ai.Header.Length);
                var positionInRecord = ByteConverter.IntToByteArray(ai.Header.PositionInRecord);
                var nrOrder = ByteConverter.IntToByteArray(ai.Header.RealLength);
                var typeId = ByteConverter.IntToByteArray(ai.AttributeTypeId);

                fullArray[fieldIndex] = Combine(SizeOfName, aiName, attLength, positionInRecord, nrOrder, typeId);

                fieldIndex++;
            }

            return Combine(fullArray);
        }

        public void SerializeType(SqoTypeInfo ti)
        {
            file.Write(0, SerializeTypeToBuffer(ti));
        }
#if ASYNC
        public async Task SerializeTypeAsync(SqoTypeInfo ti)
        {
            await file.WriteAsync(0, SerializeTypeToBuffer(ti)).ConfigureAwait(false);
        }
#endif
        private SqoTypeInfo DeserializeSqoTypeInfoFromBuffer(byte[] readFullSqoTypeInfo, bool loadRealType)
        {
            var tInfo = new SqoTypeInfo();
            tInfo.Header.headerSize = readFullSqoTypeInfo.Length;
            try
            {
                //reader.Close();
                var typeNameSize = GetBuffer(readFullSqoTypeInfo, 4, 4);
                tInfo.Header.typeNameSize = ByteConverter.ByteArrayToInt(typeNameSize);
                //read versionFirst
                var version = GetBuffer(readFullSqoTypeInfo, tInfo.Header.typeNameSize + 28, 4);
                tInfo.Header.version = ByteConverter.ByteArrayToInt(version);

                var typeName = GetBuffer(readFullSqoTypeInfo, 8, tInfo.Header.typeNameSize);
                tInfo.TypeName =
                    (string)ByteConverter.DeserializeValueType(typeof(string), typeName, tInfo.Header.version);

                if (loadRealType)
                {
#if SILVERLIGHT
                        string[] tinfoArr = tInfo.TypeName.Split(',');
                        string fullTypeName = tInfo.TypeName;
                        if (tinfoArr.Length == 2 && !tInfo.TypeName.StartsWith("sqoDB.Indexes.BTreeNode") && !tInfo.TypeName.StartsWith("KeVaSt.BTreeNode"))//written with .net version
                        {
                            fullTypeName = tInfo.TypeName + ", Version=0.0.0.1,Culture=neutral, PublicKeyToken=null";
                            tInfo.Type = Type.GetType(fullTypeName);
                            tInfo.TypeName = tInfo.Type.AssemblyQualifiedName;
                        }
                        else
                        {
                            tInfo.Type = Type.GetType(fullTypeName);
                        }

#else
                    string[] tinfoArr = null;
                    var indexOfGenericsEnd = tInfo.TypeName.LastIndexOf(']');
                    if (indexOfGenericsEnd > 0)
                    {
                        var substringStart = tInfo.TypeName.Substring(0, indexOfGenericsEnd);
                        var substringEnd = tInfo.TypeName.Substring(indexOfGenericsEnd,
                            tInfo.TypeName.Length - indexOfGenericsEnd);
                        tinfoArr = substringEnd.Split(',');
                        tinfoArr[0] = substringStart + "]";
                    }
                    else
                    {
                        tinfoArr = tInfo.TypeName.Split(',');
                    }

                    var fullTypeName = tInfo.TypeName;
                    if (tinfoArr.Length > 2 && !tInfo.TypeName.StartsWith("sqoDB.Indexes.BTreeNode") &&
                        !tInfo.TypeName.StartsWith("KeVaSt.BTreeNode")) //written with Silevrlight version
                    {
                        fullTypeName = tinfoArr[0] + "," + tinfoArr[1];
                        tInfo.Type = Type.GetType(fullTypeName);
                        tInfo.TypeName = fullTypeName;
                    }
                    else
                    {
                        tInfo.Type = Type.GetType(tInfo.TypeName);
                    }
#endif
                }

                var lastUpdate = GetBuffer(readFullSqoTypeInfo, tInfo.Header.typeNameSize + 8, 8);
                tInfo.Header.lastUpdated =
                    (DateTime)ByteConverter.DeserializeValueType(typeof(DateTime), lastUpdate, tInfo.Header.version);

                var nrRecords = GetBuffer(readFullSqoTypeInfo, tInfo.Header.typeNameSize + 16, 4);
                tInfo.Header.numberOfRecords = ByteConverter.ByteArrayToInt(nrRecords);

                var positionFirstRecord = GetBuffer(readFullSqoTypeInfo, tInfo.Header.typeNameSize + 20, 4);
                tInfo.Header.positionFirstRecord = ByteConverter.ByteArrayToInt(positionFirstRecord);

                var lengthRecord = GetBuffer(readFullSqoTypeInfo, tInfo.Header.typeNameSize + 24, 4);
                tInfo.Header.lengthOfRecord = ByteConverter.ByteArrayToInt(lengthRecord);


                var currentPosition = tInfo.Header.typeNameSize + 32;
                var nrFields = GetBuffer(readFullSqoTypeInfo, currentPosition, 4);
                tInfo.Header.NrFields = ByteConverter.ByteArrayToInt(nrFields);

                if (tInfo.Header.version <= -30) //version >= 3.0
                {
                    currentPosition += 4;
                    var TID = GetBuffer(readFullSqoTypeInfo, currentPosition, 4);
                    tInfo.Header.TID = ByteConverter.ByteArrayToInt(TID);

                    currentPosition += 4;
                    var unused1 = GetBuffer(readFullSqoTypeInfo, currentPosition, 4);
                    tInfo.Header.Unused1 = ByteConverter.ByteArrayToInt(unused1);

                    currentPosition += 4;
                    var unused2 = GetBuffer(readFullSqoTypeInfo, currentPosition, 4);
                    tInfo.Header.Unused2 = ByteConverter.ByteArrayToInt(unused2);

                    currentPosition += 4;
                    var unused3 = GetBuffer(readFullSqoTypeInfo, currentPosition, 4);
                    tInfo.Header.Unused3 = ByteConverter.ByteArrayToInt(unused3);
                }


                for (var i = 0; i < tInfo.Header.NrFields; i++)
                {
                    var ai = new FieldSqoInfo();
                    var currentPositionField = i * MetaExtractor.FieldSize + currentPosition + 4;
                    var sizeOfName = GetBuffer(readFullSqoTypeInfo, currentPositionField, 4);
                    ai.Header.SizeOfName = ByteConverter.ByteArrayToInt(sizeOfName);

                    currentPositionField += 4;
                    var name = GetBuffer(readFullSqoTypeInfo, currentPositionField, ai.Header.SizeOfName);
                    ai.Name = (string)ByteConverter.DeserializeValueType(typeof(string), name, tInfo.Header.version);

                    currentPositionField += 200;
                    var length = GetBuffer(readFullSqoTypeInfo, currentPositionField, 4);
                    ai.Header.Length = ByteConverter.ByteArrayToInt(length);

                    currentPositionField += 4;
                    var position = GetBuffer(readFullSqoTypeInfo, currentPositionField, 4);
                    ai.Header.PositionInRecord = ByteConverter.ByteArrayToInt(position);

                    currentPositionField += 4;
                    var nrOrder = GetBuffer(readFullSqoTypeInfo, currentPositionField, 4);
                    ai.Header.RealLength = ByteConverter.ByteArrayToInt(nrOrder);

                    currentPositionField += 4;
                    var typeId = GetBuffer(readFullSqoTypeInfo, currentPositionField, 4);
                    ai.AttributeTypeId = ByteConverter.ByteArrayToInt(typeId);

                    if (loadRealType)
                    {
                        ai.FInfo = MetaExtractor.FindField(tInfo.Type, ai.Name);
                        MetaExtractor.FindAddConstraints(tInfo, ai);
                        MetaExtractor.FindAddIndexes(tInfo, ai);
                    }

                    if (ai.AttributeTypeId == MetaExtractor.complexID ||
                        ai.AttributeTypeId == MetaExtractor.dictionaryID ||
                        ai.AttributeTypeId == MetaExtractor.documentID)
                    {
                        if (loadRealType) ai.AttributeType = ai.FInfo.FieldType;
                    }
                    else if (ai.Header.Length - 1 == MetaExtractor.GetSizeOfField(ai.AttributeTypeId)) //is Nullable<>
                    {
                        var fGen = typeof(Nullable<>);
                        ai.AttributeType = fGen.MakeGenericType(Cache.Cache.GetTypebyID(ai.AttributeTypeId));
                    }
                    else if (MetaExtractor.IsTextType(ai.AttributeTypeId))
                    {
                        ai.AttributeType = typeof(string);
                        ai.IsText = true;
                    }
                    else if (ai.AttributeTypeId > MetaExtractor.ArrayTypeIDExtra) //is IList<> or Array
                    {
                        if (loadRealType)
                        {
                            ai.AttributeType = ai.FInfo.FieldType;
                        }
                        else
                        {
                            if (ai.AttributeTypeId - MetaExtractor.ArrayTypeIDExtra == MetaExtractor.complexID
                                || ai.AttributeTypeId - MetaExtractor.ArrayTypeIDExtra == MetaExtractor.jaggedArrayID)
                            {
                            }
                            else
                            {
                                var elementType = Cache.Cache.GetTypebyID(ai.AttributeTypeId);
#if CF
                                ai.AttributeType = Array.CreateInstance(elementType,0).GetType();
#else
                                ai.AttributeType = elementType.MakeArrayType();
#endif
                            }
                        }

                        if (ai.AttributeTypeId - MetaExtractor.ArrayTypeIDExtra == MetaExtractor.textID)
                            ai.IsText = true;
                    }
                    else if (ai.AttributeTypeId > MetaExtractor.FixedArrayTypeId) //is IList<> or Array
                    {
                        if (loadRealType)
                        {
                            ai.AttributeType = ai.FInfo.FieldType;
                        }
                        else
                        {
                            if (ai.AttributeTypeId - MetaExtractor.FixedArrayTypeId == MetaExtractor.complexID
                                || ai.AttributeTypeId - MetaExtractor.FixedArrayTypeId == MetaExtractor.jaggedArrayID)
                            {
                            }
                            else
                            {
                                var elementType = Cache.Cache.GetTypebyID(ai.AttributeTypeId);
#if CF
                                ai.AttributeType = Array.CreateInstance(elementType,0).GetType();
#else
                                ai.AttributeType = elementType.MakeArrayType();
#endif
                            }
                        }
                    }
                    else
                    {
                        ai.AttributeType = Cache.Cache.GetTypebyID(ai.AttributeTypeId);
                    }

                    tInfo.Fields.Add(ai);
                }


                return tInfo;
            }


            catch (Exception ex)
            {
                SiaqodbConfigurator.LogMessage(
                    "File:" + filePath + " is not a valid Siaqodb database file,skipped; error:" + ex,
                    VerboseLevel.Info);
            }

            return null;
        }

        public SqoTypeInfo DeserializeSqoTypeInfo(bool loadRealType)
        {
            var headerSizeB = new byte[4];
            file.Read(0, headerSizeB);
            var headerSize = ByteConverter.ByteArrayToInt(headerSizeB);
            var readFullSqoTypeInfo = new byte[headerSize];
            file.Read(0, readFullSqoTypeInfo);
            return DeserializeSqoTypeInfoFromBuffer(readFullSqoTypeInfo, loadRealType);
        }
#if ASYNC

        public async Task<SqoTypeInfo> DeserializeSqoTypeInfoAsync(bool loadRealType)
        {
            var headerSizeB = new byte[4];
            await file.ReadAsync(0, headerSizeB).ConfigureAwait(false);
            var headerSize = ByteConverter.ByteArrayToInt(headerSizeB);
            var readFullSqoTypeInfo = new byte[headerSize];
            await file.ReadAsync(0, readFullSqoTypeInfo).ConfigureAwait(false);
            return DeserializeSqoTypeInfoFromBuffer(readFullSqoTypeInfo, loadRealType);
        }
#endif
        public void Open(bool useElevatedTrust)
        {
            file = FileFactory.Create(filePath, false, useElevatedTrust);
        }

        public void MakeEmpty()
        {
            file.Length = 0;
        }

        public void SetLength(long newLength)
        {
            file.Length = newLength;
        }

        public void Close()
        {
            file.Flush();
            file.Close();
        }
#if ASYNC
        public async Task CloseAsync()
        {
            await file.FlushAsync().ConfigureAwait(false);
            file.Close();
        }
#endif
        public void Flush()
        {
            lock (file)
            {
                file.Flush();
            }
        }
#if ASYNC
        public async Task FlushAsync()
        {
            await file.FlushAsync().ConfigureAwait(false);
        }
#endif
        private byte[] GetBuffer(byte[] readFullSqoTypeInfo, int position, int size)
        {
            var b = new byte[size];
            Array.Copy(readFullSqoTypeInfo, position, b, 0, size);
            return b;
        }

        public static byte[] Combine(params byte[][] arrays)
        {
            var totalLegth = 0;
            foreach (var data in arrays) totalLegth += data.Length;
            var ret = new byte[totalLegth];
            var offset = 0;
            foreach (var data in arrays)
            {
                Array.Copy(data, 0, ret, offset, data.Length);
                offset += data.Length;
            }

            return ret;
        }

        private FieldSqoInfo FindField(List<FieldSqoInfo> list, string fieldName)
        {
            foreach (var fi in list)
                if (string.Compare(fi.Name, fieldName) == 0)
                    return fi;
            return null;
        }
    }

    internal class ComplexObjectEventArgs : EventArgs
    {
        public ComplexObjectEventArgs(object obj, bool returnOnlyOid)
        {
            ComplexObject = obj;
            ReturnOnlyOid_TID = returnOnlyOid;
        }

        public ComplexObjectEventArgs(int OID, int TID)
        {
            this.TID = TID;
            SavedOID = OID;
        }

        public ComplexObjectEventArgs(bool justSetOID, ObjectInfo objInfo)
        {
            JustSetOID = justSetOID;
            ObjInfo = objInfo;
        }

        public object ComplexObject { get; set; }
        public Type ParentType { get; set; }
        public string FieldName { get; set; }
        public ObjectInfo ObjInfo { get; set; }
        public int SavedOID { get; set; }
        public int TID { get; set; }
        public bool ReturnOnlyOid_TID { get; set; }
        public bool JustSetOID { get; set; }
    }

    internal class DocumentEventArgs : EventArgs
    {
        public object ParentObject { get; set; }
        public string FieldName { get; set; }
        public int DocumentInfoOID { get; set; }
        public SqoTypeInfo TypeInfo { get; set; }
    }
#if ASYNC
    internal delegate Task ComplexObjectEventHandler(object sender, ComplexObjectEventArgs args);
#endif
}