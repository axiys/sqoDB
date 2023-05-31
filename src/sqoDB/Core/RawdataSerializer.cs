using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using sqoDB.Exceptions;
using sqoDB.Meta;
using sqoDB.MetaObjects;
using sqoDB.Utilities;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Core
{
    internal class RawdataSerializer
    {
        private static readonly object _syncRoot = new object();

        private static readonly Dictionary<string, ISqoFile> filesCache = new Dictionary<string, ISqoFile>();
        private bool _transactionCommitStarted;
        private ISqoFile file;
        private readonly RawdataManager manager;


        private readonly StorageEngine storageEngine;
        private readonly bool useElevatedTrust;

        public RawdataSerializer(StorageEngine storageEngine, bool useElevatedTrust)
        {
            this.storageEngine = storageEngine;
            this.useElevatedTrust = useElevatedTrust;
            manager = new RawdataManager(this.storageEngine);
        }

        public ISqoFile File
        {
            get
            {
                lock (_syncRoot)
                {
                    if (file == null)
                    {
                        if (filesCache.ContainsKey(storageEngine.path))
                        {
                            file = filesCache[storageEngine.path];
                        }
                        else
                        {
                            if (SiaqodbConfigurator.EncryptedDatabase)
                                file = FileFactory.Create(
                                    storageEngine.path + Path.DirectorySeparatorChar + "rawdata.esqr", false,
                                    useElevatedTrust);
                            else
                                file = FileFactory.Create(
                                    storageEngine.path + Path.DirectorySeparatorChar + "rawdata.sqr", false,
                                    useElevatedTrust);
                            filesCache.Add(storageEngine.path, file);
                        }
                    }

                    return file;
                }
            }
        }

        public byte[] SerializeArray(object obj, Type objectType, int length, int realLength, int dbVersion,
            ATuple<int, int> arrayMeta, ObjectSerializer objSerializer, bool elementIsText)
        {
            var b = new byte[length];
            if (obj == null)
            {
                b[0] = 1; //is null
                return b;
            }

            b[0] = 0;


            var arrayInfo = SerializeArray(obj, objectType, objSerializer, dbVersion, elementIsText);

            var rinfo = GetNewRawinfo(arrayMeta, arrayInfo.rawArray.Length, length - MetaExtractor.ExtraSizeForArray,
                arrayInfo.NrElements);
            var rawOID = rinfo.OID;

            var rawOIDBytes = ByteConverter.SerializeValueType(rawOID, typeof(int), dbVersion);
            var nrElementsBytes = ByteConverter.SerializeValueType(arrayInfo.NrElements, typeof(int), dbVersion);
            Array.Copy(rawOIDBytes, 0, b, 1, rawOIDBytes.Length);
            Array.Copy(nrElementsBytes, 0, b, rawOIDBytes.Length + 1, nrElementsBytes.Length);


            File.Write(rinfo.Position, arrayInfo.rawArray);

            return b;
        }
#if ASYNC
        public async Task<byte[]> SerializeArrayAsync(object obj, Type objectType, int length, int realLength,
            int dbVersion, ATuple<int, int> arrayMeta, ObjectSerializer objSerializer, bool elementIsText)
        {
            var b = new byte[length];
            if (obj == null)
            {
                b[0] = 1; //is null
                return b;
            }

            b[0] = 0;


            var arrayInfo = await SerializeArrayAsync(obj, objectType, objSerializer, dbVersion, elementIsText)
                .ConfigureAwait(false);

            var rinfo = await GetNewRawinfoAsync(arrayMeta, arrayInfo.rawArray.Length,
                length - MetaExtractor.ExtraSizeForArray, arrayInfo.NrElements).ConfigureAwait(false);
            var rawOID = rinfo.OID;

            var rawOIDBytes = ByteConverter.SerializeValueType(rawOID, typeof(int), dbVersion);
            var nrElementsBytes = ByteConverter.SerializeValueType(arrayInfo.NrElements, typeof(int), dbVersion);
            Array.Copy(rawOIDBytes, 0, b, 1, rawOIDBytes.Length);
            Array.Copy(nrElementsBytes, 0, b, rawOIDBytes.Length + 1, nrElementsBytes.Length);


            await File.WriteAsync(rinfo.Position, arrayInfo.rawArray).ConfigureAwait(false);

            return b;
        }
#endif

        private ArrayInfo SerializeArray(object obj, Type objectType, ObjectSerializer objSerializer, int dbVersion,
            bool elementIsText)
        {
            var arrInfo = new ArrayInfo();

            if (objectType == typeof(string)) //text
            {
                arrInfo.NrElements = Encoding.UTF8.GetByteCount((string)obj);

                //optimize serialization and gets directly bytes of string not serialize every byte
                arrInfo.rawArray = ByteConverter.SerializeValueType(obj, objectType,
                    MetaHelper.PaddingSize(arrInfo.NrElements), arrInfo.NrElements, dbVersion);
            }
            else if (objectType == typeof(byte[]))
            {
                arrInfo.NrElements = ((byte[])obj).Length;
                //optimize serialization and gets directly bytes of string not serialize every byte
                arrInfo.rawArray = ByteConverter.SerializeValueType(obj, objectType,
                    MetaHelper.PaddingSize(arrInfo.NrElements), arrInfo.NrElements, dbVersion);
            }
            else
            {
                Type elementType = null;
                if (objectType.IsGenericType())
                    elementType = objectType.GetProperty("Item").PropertyType;
                else
                    elementType = objectType.GetElementType();

                var elementTypeId = MetaExtractor.GetAttributeType(elementType);
                if (typeof(IList).IsAssignableFrom(elementType))
                    elementTypeId = MetaExtractor.jaggedArrayID;
                else if (elementIsText && elementType == typeof(string)) elementTypeId = MetaExtractor.textID;
                var elementSize = MetaExtractor.GetSizeOfField(elementTypeId);

                arrInfo.NrElements = ((IList)obj).Count;
                arrInfo.ElementTypeId = elementTypeId;
                var rawLength = arrInfo.NrElements * elementSize;
                var rawArray = new byte[rawLength];
                //build array for elements
                var currentIndex = 0;
                foreach (var elem in (IList)obj)
                {
                    byte[] elemArray = null;
                    if (elementTypeId == MetaExtractor.complexID)
                    {
                        elemArray = objSerializer.GetComplexObjectBytes(elem);
                    }
                    else if (elementTypeId == MetaExtractor.jaggedArrayID)
                    {
                        elemArray = new byte[elementSize];
                        if (elem == null)
                        {
                            elemArray[0] = 1;
                        }
                        else
                        {
                            var arrElemInfo = SerializeArray(elem, elementType, objSerializer, dbVersion,
                                elementIsText);
                            var jaggedArray = arrElemInfo.rawArray;

                            var jaggedArrayLength = jaggedArray.Length + elementSize;

                            elemArray = new byte[jaggedArrayLength];

                            #region jaggedArrayMetaData

                            var nrElementsBytes =
                                ByteConverter.SerializeValueType(arrElemInfo.NrElements, typeof(int), dbVersion);
                            var elemTypeIdBytes =
                                ByteConverter.SerializeValueType(arrElemInfo.ElementTypeId, typeof(int), dbVersion);
                            var elemArrayLengthBytes =
                                ByteConverter.SerializeValueType(jaggedArrayLength, typeof(int), dbVersion);
                            var index = 1;
                            Array.Copy(nrElementsBytes, 0, elemArray, index, nrElementsBytes.Length);
                            index += nrElementsBytes.Length;
                            Array.Copy(elemTypeIdBytes, 0, elemArray, index, elemTypeIdBytes.Length);
                            index += elemTypeIdBytes.Length;
                            Array.Copy(elemArrayLengthBytes, 0, elemArray, index, elemArrayLengthBytes.Length);

                            #endregion


                            Array.Copy(jaggedArray, 0, elemArray, elementSize, jaggedArray.Length);
                        }

                        if (rawArray.Length - currentIndex < elemArray.Length)
                            Array.Resize(ref rawArray, elemArray.Length + currentIndex);
                    }
                    else if (elementTypeId == MetaExtractor.textID)
                    {
                        elemArray = new byte[elementSize];
                        if (elem == null)
                        {
                            elemArray[0] = 1;
                        }
                        else
                        {
                            var nrChars = Encoding.UTF8.GetByteCount((string)elem);

                            var jaggedArray = ByteConverter.SerializeValueType(elem, typeof(string),
                                MetaHelper.PaddingSize(nrChars), nrChars, dbVersion);


                            var jaggedArrayLength = jaggedArray.Length + elementSize;

                            elemArray = new byte[jaggedArrayLength];

                            #region jaggedArrayMetaData

                            var nrElementsBytes = ByteConverter.SerializeValueType(nrChars, typeof(int), dbVersion);
                            var elemTypeIdBytes =
                                ByteConverter.SerializeValueType(elementTypeId, typeof(int), dbVersion);
                            var elemArrayLengthBytes =
                                ByteConverter.SerializeValueType(jaggedArrayLength, typeof(int), dbVersion);
                            var index = 1;
                            Array.Copy(nrElementsBytes, 0, elemArray, index, nrElementsBytes.Length);
                            index += nrElementsBytes.Length;
                            Array.Copy(elemTypeIdBytes, 0, elemArray, index, elemTypeIdBytes.Length);
                            index += elemTypeIdBytes.Length;
                            Array.Copy(elemArrayLengthBytes, 0, elemArray, index, elemArrayLengthBytes.Length);

                            #endregion


                            Array.Copy(jaggedArray, 0, elemArray, elementSize, jaggedArray.Length);
                        }

                        if (rawArray.Length - currentIndex < elemArray.Length)
                            Array.Resize(ref rawArray, elemArray.Length + currentIndex);
                    }
                    else if (elem is IDictionary)
                    {
                        throw new NotSupportedTypeException(
                            "IDictionary it is not supported type as IList element type.");
                    }
                    else
                    {
                        var elemObj = elem;
                        if (elem == null)
                            if (elementType == typeof(string))
                                elemObj = string.Empty;
                        elemArray = ByteConverter.SerializeValueType(elemObj, elemObj.GetType(), elementSize,
                            MetaExtractor.GetAbsoluteSizeOfField(elementTypeId), dbVersion);
                    }

                    Array.Copy(elemArray, 0, rawArray, currentIndex, elemArray.Length);

                    currentIndex += elemArray.Length;
                }

                arrInfo.rawArray = rawArray;
            }

            return arrInfo;
        }
#if ASYNC
        private async Task<ArrayInfo> SerializeArrayAsync(object obj, Type objectType, ObjectSerializer objSerializer,
            int dbVersion, bool elementIsText)
        {
            var arrInfo = new ArrayInfo();

            if (objectType == typeof(string)) //text
            {
                arrInfo.NrElements = Encoding.UTF8.GetByteCount((string)obj);

                //optimize serialization and gets directly bytes of string not serialize every byte
                arrInfo.rawArray = ByteConverter.SerializeValueType(obj, objectType,
                    MetaHelper.PaddingSize(arrInfo.NrElements), arrInfo.NrElements, dbVersion);
            }
            else if (objectType == typeof(byte[]))
            {
                arrInfo.NrElements = ((byte[])obj).Length;
                //optimize serialization and gets directly bytes of string not serialize every byte
                arrInfo.rawArray = ByteConverter.SerializeValueType(obj, objectType,
                    MetaHelper.PaddingSize(arrInfo.NrElements), arrInfo.NrElements, dbVersion);
            }
            else
            {
                Type elementType = null;
                if (objectType.IsGenericType())
                    elementType = objectType.GetProperty("Item").PropertyType;
                else
                    elementType = objectType.GetElementType();

                var elementTypeId = MetaExtractor.GetAttributeType(elementType);
                if (typeof(IList).IsAssignableFrom(elementType))
                    elementTypeId = MetaExtractor.jaggedArrayID;
                else if (elementIsText && elementType == typeof(string)) elementTypeId = MetaExtractor.textID;
                var elementSize = MetaExtractor.GetSizeOfField(elementTypeId);

                arrInfo.NrElements = ((IList)obj).Count;
                arrInfo.ElementTypeId = elementTypeId;
                var rawLength = arrInfo.NrElements * elementSize;
                var rawArray = new byte[rawLength];
                //build array for elements
                var currentIndex = 0;
                foreach (var elem in (IList)obj)
                {
                    byte[] elemArray = null;
                    if (elementTypeId == MetaExtractor.complexID)
                    {
                        elemArray = await objSerializer.GetComplexObjectBytesAsync(elem).ConfigureAwait(false);
                    }
                    else if (elementTypeId == MetaExtractor.jaggedArrayID)
                    {
                        elemArray = new byte[elementSize];
                        if (elem == null)
                        {
                            elemArray[0] = 1;
                        }
                        else
                        {
                            var arrElemInfo =
                                await SerializeArrayAsync(elem, elementType, objSerializer, dbVersion, elementIsText)
                                    .ConfigureAwait(false);
                            var jaggedArray = arrElemInfo.rawArray;

                            var jaggedArrayLength = jaggedArray.Length + elementSize;

                            elemArray = new byte[jaggedArrayLength];

                            #region jaggedArrayMetaData

                            var nrElementsBytes =
                                ByteConverter.SerializeValueType(arrElemInfo.NrElements, typeof(int), dbVersion);
                            var elemTypeIdBytes =
                                ByteConverter.SerializeValueType(arrElemInfo.ElementTypeId, typeof(int), dbVersion);
                            var elemArrayLengthBytes =
                                ByteConverter.SerializeValueType(jaggedArrayLength, typeof(int), dbVersion);
                            var index = 1;
                            Array.Copy(nrElementsBytes, 0, elemArray, index, nrElementsBytes.Length);
                            index += nrElementsBytes.Length;
                            Array.Copy(elemTypeIdBytes, 0, elemArray, index, elemTypeIdBytes.Length);
                            index += elemTypeIdBytes.Length;
                            Array.Copy(elemArrayLengthBytes, 0, elemArray, index, elemArrayLengthBytes.Length);

                            #endregion


                            Array.Copy(jaggedArray, 0, elemArray, elementSize, jaggedArray.Length);
                        }

                        if (rawArray.Length - currentIndex < elemArray.Length)
                            Array.Resize(ref rawArray, elemArray.Length + currentIndex);
                    }
                    else if (elementTypeId == MetaExtractor.textID)
                    {
                        elemArray = new byte[elementSize];
                        if (elem == null)
                        {
                            elemArray[0] = 1;
                        }
                        else
                        {
                            var nrChars = Encoding.UTF8.GetByteCount((string)elem);

                            var jaggedArray = ByteConverter.SerializeValueType(elem, typeof(string),
                                MetaHelper.PaddingSize(nrChars), nrChars, dbVersion);


                            var jaggedArrayLength = jaggedArray.Length + elementSize;

                            elemArray = new byte[jaggedArrayLength];

                            #region jaggedArrayMetaData

                            var nrElementsBytes = ByteConverter.SerializeValueType(nrChars, typeof(int), dbVersion);
                            var elemTypeIdBytes =
                                ByteConverter.SerializeValueType(elementTypeId, typeof(int), dbVersion);
                            var elemArrayLengthBytes =
                                ByteConverter.SerializeValueType(jaggedArrayLength, typeof(int), dbVersion);
                            var index = 1;
                            Array.Copy(nrElementsBytes, 0, elemArray, index, nrElementsBytes.Length);
                            index += nrElementsBytes.Length;
                            Array.Copy(elemTypeIdBytes, 0, elemArray, index, elemTypeIdBytes.Length);
                            index += elemTypeIdBytes.Length;
                            Array.Copy(elemArrayLengthBytes, 0, elemArray, index, elemArrayLengthBytes.Length);

                            #endregion


                            Array.Copy(jaggedArray, 0, elemArray, elementSize, jaggedArray.Length);
                        }

                        if (rawArray.Length - currentIndex < elemArray.Length)
                            Array.Resize(ref rawArray, elemArray.Length + currentIndex);
                    }
                    else if (elem is IDictionary)
                    {
                        throw new NotSupportedTypeException(
                            "IDictionary it is not supported type as IList element type.");
                    }
                    else
                    {
                        var elemObj = elem;
                        if (elem == null)
                            if (elementType == typeof(string))
                                elemObj = string.Empty;
                        elemArray = ByteConverter.SerializeValueType(elemObj, elemObj.GetType(), elementSize,
                            MetaExtractor.GetAbsoluteSizeOfField(elementTypeId), dbVersion);
                    }

                    Array.Copy(elemArray, 0, rawArray, currentIndex, elemArray.Length);

                    currentIndex += elemArray.Length;
                }

                arrInfo.rawArray = rawArray;
            }

            return arrInfo;
        }

#endif

        public byte[] SerializeDictionary(object obj, int length, int dbVersion, DictionaryInfo dictInfo,
            ObjectSerializer objSerializer)
        {
            var b = new byte[length];
            if (obj == null)
            {
                b[0] = 1; //is null
                return b;
            }

            b[0] = 0;

            var nrElements = 0;
            var rawLength = 0;
            byte[] rawArray = null;

            nrElements = ((IDictionary)obj).Keys.Count;

            rawLength = nrElements * (MetaExtractor.GetSizeOfField(dictInfo.KeyTypeId) +
                                      MetaExtractor.GetSizeOfField(dictInfo.ValueTypeId));
            rawArray = new byte[rawLength];
            //build array for elements
            var currentIndex = 0;
            var dictionary = (IDictionary)obj;
            foreach (var elem in dictionary.Keys)
            {
                byte[] keyArray = null;
                byte[] valueArray = null;

                #region key

                if (dictInfo.KeyTypeId == MetaExtractor.complexID)
                    keyArray = objSerializer.GetComplexObjectBytes(elem);
                else if (elem is IList)
                    throw new NotSupportedTypeException(
                        "Array/IList as Type of Key of a Dictionary it is not supported");
                else
                    keyArray = ByteConverter.SerializeValueType(elem, elem.GetType(),
                        MetaExtractor.GetSizeOfField(dictInfo.KeyTypeId),
                        MetaExtractor.GetAbsoluteSizeOfField(dictInfo.KeyTypeId), dbVersion);
                Array.Copy(keyArray, 0, rawArray, currentIndex, keyArray.Length);
                currentIndex += keyArray.Length;

                #endregion

                #region value

                if (dictInfo.ValueTypeId == MetaExtractor.complexID)
                    valueArray = objSerializer.GetComplexObjectBytes(dictionary[elem]);
                else if (dictionary[elem] is IList)
                    throw new NotSupportedTypeException(
                        "Array/IList as Type of Value of a Dictionary it is not supported");
                else
                    valueArray = ByteConverter.SerializeValueType(dictionary[elem], dictionary[elem].GetType(),
                        MetaExtractor.GetSizeOfField(dictInfo.ValueTypeId),
                        MetaExtractor.GetAbsoluteSizeOfField(dictInfo.ValueTypeId), dbVersion);
                Array.Copy(valueArray, 0, rawArray, currentIndex, valueArray.Length);
                currentIndex += valueArray.Length;

                #endregion
            }

            var arrayMeta = new ATuple<int, int>(dictInfo.RawOID, nrElements);
            var rinfo = GetNewRawinfo(arrayMeta, rawLength, 0,
                nrElements); //element length does not matter because it's stored in place by dictionaryInfo
            var rawOID = rinfo.OID;

            var rawOIDBytes = ByteConverter.SerializeValueType(rawOID, typeof(int), dbVersion);
            var nrElementsBytes = ByteConverter.SerializeValueType(nrElements, typeof(int), dbVersion);
            var keyTypeIdBytes = ByteConverter.SerializeValueType(dictInfo.KeyTypeId, typeof(int), dbVersion);
            var valueTypeIdBytes = ByteConverter.SerializeValueType(dictInfo.ValueTypeId, typeof(int), dbVersion);

            var index = 1;
            Array.Copy(rawOIDBytes, 0, b, index, rawOIDBytes.Length);
            index += rawOIDBytes.Length;
            Array.Copy(nrElementsBytes, 0, b, index, nrElementsBytes.Length);
            index += nrElementsBytes.Length;
            Array.Copy(keyTypeIdBytes, 0, b, index, keyTypeIdBytes.Length);
            index += keyTypeIdBytes.Length;
            Array.Copy(valueTypeIdBytes, 0, b, index, valueTypeIdBytes.Length);

            File.Write(rinfo.Position, rawArray);

            return b;
        }
#if ASYNC
        public async Task<byte[]> SerializeDictionaryAsync(object obj, int length, int dbVersion,
            DictionaryInfo dictInfo, ObjectSerializer objSerializer)
        {
            var b = new byte[length];
            if (obj == null)
            {
                b[0] = 1; //is null
                return b;
            }

            b[0] = 0;

            var nrElements = 0;
            var rawLength = 0;
            byte[] rawArray = null;

            nrElements = ((IDictionary)obj).Keys.Count;

            rawLength = nrElements * (MetaExtractor.GetSizeOfField(dictInfo.KeyTypeId) +
                                      MetaExtractor.GetSizeOfField(dictInfo.ValueTypeId));
            rawArray = new byte[rawLength];
            //build array for elements
            var currentIndex = 0;
            var dictionary = (IDictionary)obj;
            foreach (var elem in dictionary.Keys)
            {
                byte[] keyArray = null;
                byte[] valueArray = null;

                #region key

                if (dictInfo.KeyTypeId == MetaExtractor.complexID)
                    keyArray = await objSerializer.GetComplexObjectBytesAsync(elem).ConfigureAwait(false);
                else if (elem is IList)
                    throw new NotSupportedTypeException(
                        "Array/IList as Type of Key of a Dictionary it is not supported");
                else
                    keyArray = ByteConverter.SerializeValueType(elem, elem.GetType(),
                        MetaExtractor.GetSizeOfField(dictInfo.KeyTypeId),
                        MetaExtractor.GetAbsoluteSizeOfField(dictInfo.KeyTypeId), dbVersion);
                Array.Copy(keyArray, 0, rawArray, currentIndex, keyArray.Length);
                currentIndex += keyArray.Length;

                #endregion

                #region value

                if (dictInfo.ValueTypeId == MetaExtractor.complexID)
                    valueArray = await objSerializer.GetComplexObjectBytesAsync(dictionary[elem]).ConfigureAwait(false);
                else if (dictionary[elem] is IList)
                    throw new NotSupportedTypeException(
                        "Array/IList as Type of Value of a Dictionary it is not supported");
                else
                    valueArray = ByteConverter.SerializeValueType(dictionary[elem], dictionary[elem].GetType(),
                        MetaExtractor.GetSizeOfField(dictInfo.ValueTypeId),
                        MetaExtractor.GetAbsoluteSizeOfField(dictInfo.ValueTypeId), dbVersion);
                Array.Copy(valueArray, 0, rawArray, currentIndex, valueArray.Length);
                currentIndex += valueArray.Length;

                #endregion
            }

            var arrayMeta = new ATuple<int, int>(dictInfo.RawOID, nrElements);
            var rinfo = await GetNewRawinfoAsync(arrayMeta, rawLength, 0, nrElements)
                .ConfigureAwait(false); //element length does not matter because it's stored in place by dictionaryInfo
            var rawOID = rinfo.OID;

            var rawOIDBytes = ByteConverter.SerializeValueType(rawOID, typeof(int), dbVersion);
            var nrElementsBytes = ByteConverter.SerializeValueType(nrElements, typeof(int), dbVersion);
            var keyTypeIdBytes = ByteConverter.SerializeValueType(dictInfo.KeyTypeId, typeof(int), dbVersion);
            var valueTypeIdBytes = ByteConverter.SerializeValueType(dictInfo.ValueTypeId, typeof(int), dbVersion);

            var index = 1;
            Array.Copy(rawOIDBytes, 0, b, index, rawOIDBytes.Length);
            index += rawOIDBytes.Length;
            Array.Copy(nrElementsBytes, 0, b, index, nrElementsBytes.Length);
            index += nrElementsBytes.Length;
            Array.Copy(keyTypeIdBytes, 0, b, index, keyTypeIdBytes.Length);
            index += keyTypeIdBytes.Length;
            Array.Copy(valueTypeIdBytes, 0, b, index, valueTypeIdBytes.Length);

            await File.WriteAsync(rinfo.Position, rawArray).ConfigureAwait(false);

            return b;
        }
#endif

        public object DeserializeDictionary(Type objectType, byte[] bytes, int dbVersion,
            ObjectSerializer objSerializer, Type parentType, string fieldName)
        {
            if (bytes[0] == 1) //is null
                return null;
            var oidBytes = new byte[4];
            Array.Copy(bytes, 1, oidBytes, 0, 4);
            var rawInfoOID = (int)ByteConverter.DeserializeValueType(typeof(int), oidBytes, dbVersion);

            var nrElemeBytes = new byte[4];
            Array.Copy(bytes, oidBytes.Length + 1, nrElemeBytes, 0, 4);
            var nrElem = (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

            var keyTypeIdBytes = new byte[4];
            Array.Copy(bytes, oidBytes.Length + nrElemeBytes.Length + 1, keyTypeIdBytes, 0, 4);
            var keyTypeId = (int)ByteConverter.DeserializeValueType(typeof(int), keyTypeIdBytes, dbVersion);

            var valueTypeIdBytes = new byte[4];
            Array.Copy(bytes, oidBytes.Length + nrElemeBytes.Length + keyTypeIdBytes.Length + 1, valueTypeIdBytes, 0,
                4);
            var valueTypeId = (int)ByteConverter.DeserializeValueType(typeof(int), valueTypeIdBytes, dbVersion);


            var info = manager.GetRawdataInfo(rawInfoOID);
            if (info == null) return null;


            var arrayData = new byte[info.Length];
            File.Read(info.Position, arrayData);

            var objToReturn = Activator.CreateInstance(objectType);
            var actualDict = (IDictionary)objToReturn;
            var currentIndex = 0;

            var keyValueType = actualDict.GetType().GetGenericArguments();
            if (keyValueType.Length != 2)
                throw new NotSupportedTypeException("Type:" + actualDict.GetType() + " is not supported");
            var keyType = keyValueType[0];
            var valueType = keyValueType[1];


            var keyLen = MetaExtractor.GetSizeOfField(keyTypeId);
            var valueLen = MetaExtractor.GetSizeOfField(valueTypeId);
            var keyBytes = new byte[keyLen];
            var valueBytes = new byte[valueLen];
            for (var i = 0; i < nrElem; i++)
            {
                object key = null;
                Array.Copy(arrayData, currentIndex, keyBytes, 0, keyLen);
                if (keyTypeId == MetaExtractor.complexID)
                    key = objSerializer.ReadComplexObject(keyBytes, parentType, fieldName);
                else
                    key = ByteConverter.DeserializeValueType(keyType, keyBytes, true, dbVersion);
                currentIndex += keyLen;

                object val = null;
                Array.Copy(arrayData, currentIndex, valueBytes, 0, valueLen);
                if (valueTypeId == MetaExtractor.complexID)
                    val = objSerializer.ReadComplexObject(valueBytes, parentType, fieldName);
                else
                    val = ByteConverter.DeserializeValueType(valueType, valueBytes, true, dbVersion);
                currentIndex += valueLen;

                actualDict.Add(key, val);
            }

            return objToReturn;
        }
#if ASYNC
        public async Task<object> DeserializeDictionaryAsync(Type objectType, byte[] bytes, int dbVersion,
            ObjectSerializer objSerializer, Type parentType, string fieldName)
        {
            if (bytes[0] == 1) //is null
                return null;
            var oidBytes = new byte[4];
            Array.Copy(bytes, 1, oidBytes, 0, 4);
            var rawInfoOID = (int)ByteConverter.DeserializeValueType(typeof(int), oidBytes, dbVersion);

            var nrElemeBytes = new byte[4];
            Array.Copy(bytes, oidBytes.Length + 1, nrElemeBytes, 0, 4);
            var nrElem = (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

            var keyTypeIdBytes = new byte[4];
            Array.Copy(bytes, oidBytes.Length + nrElemeBytes.Length + 1, keyTypeIdBytes, 0, 4);
            var keyTypeId = (int)ByteConverter.DeserializeValueType(typeof(int), keyTypeIdBytes, dbVersion);

            var valueTypeIdBytes = new byte[4];
            Array.Copy(bytes, oidBytes.Length + nrElemeBytes.Length + keyTypeIdBytes.Length + 1, valueTypeIdBytes, 0,
                4);
            var valueTypeId = (int)ByteConverter.DeserializeValueType(typeof(int), valueTypeIdBytes, dbVersion);


            var info = await manager.GetRawdataInfoAsync(rawInfoOID).ConfigureAwait(false);
            if (info == null) return null;


            var arrayData = new byte[info.Length];
            await File.ReadAsync(info.Position, arrayData).ConfigureAwait(false);

            var objToReturn = Activator.CreateInstance(objectType);
            var actualDict = (IDictionary)objToReturn;
            var currentIndex = 0;

            var keyValueType = actualDict.GetType().GetGenericArguments();
            if (keyValueType.Length != 2)
                throw new NotSupportedTypeException("Type:" + actualDict.GetType() + " is not supported");
            var keyType = keyValueType[0];
            var valueType = keyValueType[1];


            var keyLen = MetaExtractor.GetSizeOfField(keyTypeId);
            var valueLen = MetaExtractor.GetSizeOfField(valueTypeId);
            var keyBytes = new byte[keyLen];
            var valueBytes = new byte[valueLen];
            for (var i = 0; i < nrElem; i++)
            {
                object key = null;
                Array.Copy(arrayData, currentIndex, keyBytes, 0, keyLen);
                if (keyTypeId == MetaExtractor.complexID)
                    key = await objSerializer.ReadComplexObjectAsync(keyBytes, parentType, fieldName)
                        .ConfigureAwait(false);
                else
                    key = ByteConverter.DeserializeValueType(keyType, keyBytes, true, dbVersion);
                currentIndex += keyLen;

                object val = null;
                Array.Copy(arrayData, currentIndex, valueBytes, 0, valueLen);
                if (valueTypeId == MetaExtractor.complexID)
                    val = await objSerializer.ReadComplexObjectAsync(valueBytes, parentType, fieldName)
                        .ConfigureAwait(false);
                else
                    val = ByteConverter.DeserializeValueType(valueType, valueBytes, true, dbVersion);
                currentIndex += valueLen;

                actualDict.Add(key, val);
            }

            return objToReturn;
        }
#endif
        private RawdataInfo GetNewRawinfo(ATuple<int, int> arrayMeta, int rawLength, int elemLength, int nrElem)
        {
            if (arrayMeta == null || arrayMeta.Name == 0 || _transactionCommitStarted) //insert
                return GetNextFreeOne(rawLength, elemLength);

            //already exists array meta defined
            var info = manager.GetRawdataInfo(arrayMeta.Name);
            if (rawLength <= info.Length) //means has enough space
                return info;

            //find new free space with enough length
            info.IsFree = true;
            manager.SaveRawdataInfo(info);
            return GetNextFreeOne(rawLength, elemLength);
        }
#if ASYNC
        private async Task<RawdataInfo> GetNewRawinfoAsync(ATuple<int, int> arrayMeta, int rawLength, int elemLength,
            int nrElem)
        {
            if (arrayMeta == null || arrayMeta.Name == 0 || _transactionCommitStarted) //insert
                return await GetNextFreeOneAsync(rawLength, elemLength).ConfigureAwait(false);

            //already exists array meta defined
            var info = await manager.GetRawdataInfoAsync(arrayMeta.Name).ConfigureAwait(false);
            if (rawLength <= info.Length) //means has enough space
                return info;

            //find new free space with enough length
            info.IsFree = true;
            await manager.SaveRawdataInfoAsync(info).ConfigureAwait(false);
            return await GetNextFreeOneAsync(rawLength, elemLength).ConfigureAwait(false);
        }
#endif
        private RawdataInfo GetNextFreeOne(int rawLength, int elemLength)
        {
            /*RawdataInfo existingFree = manager.GetFreeRawdataInfo(rawLength);
            if (existingFree != null)
            {
                existingFree.IsFree = false;
                existingFree.Length = rawLength;
                existingFree.ElementLength = elemLength;
                manager.SaveRawdataInfo(existingFree);

                return existingFree;
            }
            else//get new one
            {*/
            var info = new RawdataInfo();
            info.Length =
                rawLength * 2; //allowing to store double number of elements to avoid allocation of new space for every new element
            info.ElementLength = elemLength;
            info.OID = manager.GetNextOID();
            long position = 0;
            if (info.OID - 1 > 0)
            {
                var prev = manager.GetRawdataInfo(info.OID - 1);
                position = prev.Position + prev.Length;
            }

            info.Position = position;
            manager.SaveRawdataInfo(info);

            return info;
            //}
        }
#if ASYNC
        private async Task<RawdataInfo> GetNextFreeOneAsync(int rawLength, int elemLength)
        {
            /*RawdataInfo existingFree = manager.GetFreeRawdataInfo(rawLength);
            if (existingFree != null)
            {
                existingFree.IsFree = false;
                existingFree.Length = rawLength;
                existingFree.ElementLength = elemLength;
                manager.SaveRawdataInfo(existingFree);

                return existingFree;
            }
            else//get new one
            {*/
            var info = new RawdataInfo();
            info.Length =
                rawLength * 2; //allowing to store double number of elements to avoid allocation of new space for every new element
            info.ElementLength = elemLength;
            info.OID = manager.GetNextOID();
            long position = 0;
            if (info.OID - 1 > 0)
            {
                var prev = await manager.GetRawdataInfoAsync(info.OID - 1).ConfigureAwait(false);
                position = prev.Position + prev.Length;
            }

            info.Position = position;
            await manager.SaveRawdataInfoAsync(info).ConfigureAwait(false);

            return info;
            //}
        }
#endif
        public object DeserializeArray(Type objectType, byte[] bytes, bool checkEncrypted, int dbVersion, bool isText,
            bool elementIsText, ObjectSerializer objSerializer, Type parentType, string fieldName)
        {
            var isArray = objectType.IsArray;

            if (bytes[0] == 1) //is null
                return null;
            var oidBytes = new byte[4];
            Array.Copy(bytes, 1, oidBytes, 0, 4);
            var rawInfoOID = (int)ByteConverter.DeserializeValueType(typeof(int), oidBytes, dbVersion);

            var nrElemeBytes = new byte[4];
            Array.Copy(bytes, MetaExtractor.ExtraSizeForArray - 4, nrElemeBytes, 0, 4);
            var nrElem = (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);


            var info = manager.GetRawdataInfo(rawInfoOID);
            if (info == null) return null;

            var arrayData = new byte[info.Length];
            File.Read(info.Position, arrayData);

            return DeserializeArrayInternal(objectType, arrayData, checkEncrypted, dbVersion, isText, elementIsText,
                nrElem, objSerializer, parentType, fieldName);
        }
#if ASYNC

        public async Task<object> DeserializeArrayAsync(Type objectType, byte[] bytes, bool checkEncrypted,
            int dbVersion, bool isText, bool elementIsText, ObjectSerializer objSerializer, Type parentType,
            string fieldName)
        {
            var isArray = objectType.IsArray;

            if (bytes[0] == 1) //is null
                return null;
            var oidBytes = new byte[4];
            Array.Copy(bytes, 1, oidBytes, 0, 4);
            var rawInfoOID = (int)ByteConverter.DeserializeValueType(typeof(int), oidBytes, dbVersion);

            var nrElemeBytes = new byte[4];
            Array.Copy(bytes, MetaExtractor.ExtraSizeForArray - 4, nrElemeBytes, 0, 4);
            var nrElem = (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);


            var info = await manager.GetRawdataInfoAsync(rawInfoOID).ConfigureAwait(false);
            if (info == null) return null;

            var arrayData = new byte[info.Length];
            await File.ReadAsync(info.Position, arrayData).ConfigureAwait(false);

            return await DeserializeArrayInternalAsync(objectType, arrayData, checkEncrypted, dbVersion, isText,
                elementIsText, nrElem, objSerializer, parentType, fieldName).ConfigureAwait(false);
        }
#endif
        private object DeserializeArrayInternal(Type objectType, byte[] arrayData, bool checkEncrypted, int dbVersion,
            bool isText, bool elementIsText, int nrElem, ObjectSerializer objSerializer, Type parentType,
            string fieldName)
        {
            var isArray = objectType.IsArray;
            var elementType = objectType.GetElementType();
            if (!isArray && !isText)
                elementType = objectType.GetProperty("Item").PropertyType;
            else if (isText) elementType = typeof(string);
            var elementTypeId = MetaExtractor.GetAttributeType(elementType);
            if (typeof(IList).IsAssignableFrom(elementType))
                elementTypeId = MetaExtractor.jaggedArrayID;
            else if (elementIsText) elementTypeId = MetaExtractor.textID;
            var elementSize = MetaExtractor.GetSizeOfField(elementTypeId);

            if (elementType == typeof(byte) && isArray) //optimize for binary data
            {
                var bytesPadded =
                    (byte[])ByteConverter.DeserializeValueType(objectType, arrayData, checkEncrypted, dbVersion);
                var realBytes = new byte[nrElem];
                Array.Copy(bytesPadded, 0, realBytes, 0, nrElem);

                return realBytes;
            }

            if (isText)
            {
                var strNrBytes = MetaHelper.PaddingSize(nrElem);
                var realBytes = new byte[strNrBytes];
                Array.Copy(arrayData, 0, realBytes, 0, strNrBytes);

                return ByteConverter.DeserializeValueType(objectType, realBytes, checkEncrypted, dbVersion);
            }

            Array ar = null;
            IList theList = null;
            if (isArray)
            {
                ar = Array.CreateInstance(elementType, nrElem);
            }
            else
            {
                var objToReturn = Activator.CreateInstance(objectType);
                theList = (IList)objToReturn;
            }

            var currentIndex = 0;
            var elemBytes = new byte[elementSize];
            for (var i = 0; i < nrElem; i++)
            {
                Array.Copy(arrayData, currentIndex, elemBytes, 0, elementSize);
                currentIndex += elementSize;
                object obj = null;
                if (elementTypeId == MetaExtractor.jaggedArrayID)
                {
                    if (elemBytes[0] == 1)
                    {
                        obj = null;
                    }
                    else
                    {
                        var nrElemeBytes = new byte[4];
                        Array.Copy(elemBytes, 1, nrElemeBytes, 0, 4);
                        var nrJaggedElem =
                            (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

                        var jaggedTypeIdBytes = new byte[4];
                        Array.Copy(elemBytes, nrElemeBytes.Length + 1, jaggedTypeIdBytes, 0, 4);
                        var jaggedTypeId =
                            (int)ByteConverter.DeserializeValueType(typeof(int), jaggedTypeIdBytes, dbVersion);

                        var elemeArraySizeBytes = new byte[4];
                        Array.Copy(elemBytes, nrElemeBytes.Length + jaggedTypeIdBytes.Length + 1, elemeArraySizeBytes,
                            0, 4);
                        var elemeArraySize =
                            (int)ByteConverter.DeserializeValueType(typeof(int), elemeArraySizeBytes, dbVersion);

                        var jaggedElemSize = MetaExtractor.GetSizeOfField(jaggedTypeId);

                        var jaggedArrayBytes = new byte[elemeArraySize - elementSize];

                        Array.Copy(arrayData, currentIndex, jaggedArrayBytes, 0, jaggedArrayBytes.Length);
                        currentIndex += jaggedArrayBytes.Length;
                        obj = DeserializeArrayInternal(elementType, jaggedArrayBytes, checkEncrypted, dbVersion, isText,
                            elementIsText, nrJaggedElem, objSerializer, parentType, fieldName);
                    }
                }
                else if (elementTypeId == MetaExtractor.textID)
                {
                    if (elemBytes[0] == 1)
                    {
                        obj = null;
                    }
                    else
                    {
                        var nrElemeBytes = new byte[4];
                        Array.Copy(elemBytes, 1, nrElemeBytes, 0, 4);
                        var nrJaggedElem =
                            (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

                        var jaggedTypeIdBytes = new byte[4];
                        Array.Copy(elemBytes, nrElemeBytes.Length + 1, jaggedTypeIdBytes, 0, 4);
                        var jaggedTypeId =
                            (int)ByteConverter.DeserializeValueType(typeof(int), jaggedTypeIdBytes, dbVersion);

                        var elemeArraySizeBytes = new byte[4];
                        Array.Copy(elemBytes, nrElemeBytes.Length + jaggedTypeIdBytes.Length + 1, elemeArraySizeBytes,
                            0, 4);
                        var elemeArraySize =
                            (int)ByteConverter.DeserializeValueType(typeof(int), elemeArraySizeBytes, dbVersion);


                        var jaggedArrayBytes = new byte[elemeArraySize - elementSize];

                        Array.Copy(arrayData, currentIndex, jaggedArrayBytes, 0, jaggedArrayBytes.Length);
                        currentIndex += jaggedArrayBytes.Length;

                        var strNrBytes = MetaHelper.PaddingSize(nrJaggedElem);
                        var realBytes = new byte[strNrBytes];
                        Array.Copy(jaggedArrayBytes, 0, realBytes, 0, strNrBytes);

                        obj = ByteConverter.DeserializeValueType(elementType, realBytes, checkEncrypted, dbVersion);
                    }
                }
                else if (elementTypeId == MetaExtractor.complexID)
                {
                    obj = objSerializer.ReadComplexObject(elemBytes, parentType, fieldName);
                }
                else
                {
                    obj = ByteConverter.DeserializeValueType(elementType, elemBytes, checkEncrypted, dbVersion);
                }

                if (isArray)
                {
                    ar.SetValue(obj, i);
                }
                else
                {
                    if (obj != null) theList.Add(obj);
                }
            }

            return ar == null ? theList : ar;
        }
#if ASYNC
        private async Task<object> DeserializeArrayInternalAsync(Type objectType, byte[] arrayData, bool checkEncrypted,
            int dbVersion, bool isText, bool elementIsText, int nrElem, ObjectSerializer objSerializer, Type parentType,
            string fieldName)
        {
            var isArray = objectType.IsArray;
            var elementType = objectType.GetElementType();
            if (!isArray && !isText)
                elementType = objectType.GetProperty("Item").PropertyType;
            else if (isText) elementType = typeof(string);
            var elementTypeId = MetaExtractor.GetAttributeType(elementType);
            if (typeof(IList).IsAssignableFrom(elementType))
                elementTypeId = MetaExtractor.jaggedArrayID;
            else if (elementIsText) elementTypeId = MetaExtractor.textID;
            var elementSize = MetaExtractor.GetSizeOfField(elementTypeId);

            if (elementType == typeof(byte) && isArray) //optimize for binary data
            {
                var bytesPadded =
                    (byte[])ByteConverter.DeserializeValueType(objectType, arrayData, checkEncrypted, dbVersion);
                var realBytes = new byte[nrElem];
                Array.Copy(bytesPadded, 0, realBytes, 0, nrElem);

                return realBytes;
            }

            if (isText)
            {
                var strNrBytes = MetaHelper.PaddingSize(nrElem);
                var realBytes = new byte[strNrBytes];
                Array.Copy(arrayData, 0, realBytes, 0, strNrBytes);

                return ByteConverter.DeserializeValueType(objectType, realBytes, checkEncrypted, dbVersion);
            }

            Array ar = null;
            IList theList = null;
            if (isArray)
            {
                ar = Array.CreateInstance(elementType, nrElem);
            }
            else
            {
                var objToReturn = Activator.CreateInstance(objectType);
                theList = (IList)objToReturn;
            }

            var currentIndex = 0;
            var elemBytes = new byte[elementSize];
            for (var i = 0; i < nrElem; i++)
            {
                Array.Copy(arrayData, currentIndex, elemBytes, 0, elementSize);
                currentIndex += elementSize;
                object obj = null;
                if (elementTypeId == MetaExtractor.jaggedArrayID)
                {
                    if (elemBytes[0] == 1)
                    {
                        obj = null;
                    }
                    else
                    {
                        var nrElemeBytes = new byte[4];
                        Array.Copy(elemBytes, 1, nrElemeBytes, 0, 4);
                        var nrJaggedElem =
                            (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

                        var jaggedTypeIdBytes = new byte[4];
                        Array.Copy(elemBytes, nrElemeBytes.Length + 1, jaggedTypeIdBytes, 0, 4);
                        var jaggedTypeId =
                            (int)ByteConverter.DeserializeValueType(typeof(int), jaggedTypeIdBytes, dbVersion);

                        var elemeArraySizeBytes = new byte[4];
                        Array.Copy(elemBytes, nrElemeBytes.Length + jaggedTypeIdBytes.Length + 1, elemeArraySizeBytes,
                            0, 4);
                        var elemeArraySize =
                            (int)ByteConverter.DeserializeValueType(typeof(int), elemeArraySizeBytes, dbVersion);

                        var jaggedElemSize = MetaExtractor.GetSizeOfField(jaggedTypeId);

                        var jaggedArrayBytes = new byte[elemeArraySize - elementSize];

                        Array.Copy(arrayData, currentIndex, jaggedArrayBytes, 0, jaggedArrayBytes.Length);
                        currentIndex += jaggedArrayBytes.Length;
                        obj = await DeserializeArrayInternalAsync(elementType, jaggedArrayBytes, checkEncrypted,
                                dbVersion, isText, elementIsText, nrJaggedElem, objSerializer, parentType, fieldName)
                            .ConfigureAwait(false);
                    }
                }
                else if (elementTypeId == MetaExtractor.textID)
                {
                    if (elemBytes[0] == 1)
                    {
                        obj = null;
                    }
                    else
                    {
                        var nrElemeBytes = new byte[4];
                        Array.Copy(elemBytes, 1, nrElemeBytes, 0, 4);
                        var nrJaggedElem =
                            (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

                        var jaggedTypeIdBytes = new byte[4];
                        Array.Copy(elemBytes, nrElemeBytes.Length + 1, jaggedTypeIdBytes, 0, 4);
                        var jaggedTypeId =
                            (int)ByteConverter.DeserializeValueType(typeof(int), jaggedTypeIdBytes, dbVersion);

                        var elemeArraySizeBytes = new byte[4];
                        Array.Copy(elemBytes, nrElemeBytes.Length + jaggedTypeIdBytes.Length + 1, elemeArraySizeBytes,
                            0, 4);
                        var elemeArraySize =
                            (int)ByteConverter.DeserializeValueType(typeof(int), elemeArraySizeBytes, dbVersion);


                        var jaggedArrayBytes = new byte[elemeArraySize - elementSize];

                        Array.Copy(arrayData, currentIndex, jaggedArrayBytes, 0, jaggedArrayBytes.Length);
                        currentIndex += jaggedArrayBytes.Length;

                        var strNrBytes = MetaHelper.PaddingSize(nrJaggedElem);
                        var realBytes = new byte[strNrBytes];
                        Array.Copy(jaggedArrayBytes, 0, realBytes, 0, strNrBytes);

                        obj = ByteConverter.DeserializeValueType(elementType, realBytes, checkEncrypted, dbVersion);
                    }
                }
                else if (elementTypeId == MetaExtractor.complexID)
                {
                    obj = await objSerializer.ReadComplexObjectAsync(elemBytes, parentType, fieldName)
                        .ConfigureAwait(false);
                }
                else
                {
                    obj = ByteConverter.DeserializeValueType(elementType, elemBytes, checkEncrypted, dbVersion);
                }

                if (isArray)
                {
                    ar.SetValue(obj, i);
                }
                else
                {
                    if (obj != null) theList.Add(obj);
                }
            }

            return ar == null ? theList : ar;
        }

#endif
        public object DeserializeArray(Type objectType, byte[] bytes, bool checkEncrypted, int dbVersion, bool isText,
            bool elemnIsText)
        {
            return DeserializeArray(objectType, bytes, checkEncrypted, dbVersion, isText, elemnIsText, null, null,
                null);
        }
#if ASYNC
        public async Task<object> DeserializeArrayAsync(Type objectType, byte[] bytes, bool checkEncrypted,
            int dbVersion, bool isText, bool elemnIsText)
        {
            return await DeserializeArrayAsync(objectType, bytes, checkEncrypted, dbVersion, isText, elemnIsText, null,
                null, null).ConfigureAwait(false);
        }
#endif
        public List<KeyValuePair<int, int>> ReadComplexArrayOids(byte[] bytes, int dbVersion,
            ObjectSerializer objSerializer)
        {
            var list = new List<KeyValuePair<int, int>>();

            if (bytes[0] == 1) //is null
                return list;

            var oidBytes = new byte[4];
            Array.Copy(bytes, 1, oidBytes, 0, 4);
            var rawInfoOID = (int)ByteConverter.DeserializeValueType(typeof(int), oidBytes, dbVersion);

            var nrElemeBytes = new byte[4];
            Array.Copy(bytes, MetaExtractor.ExtraSizeForArray - 4, nrElemeBytes, 0, 4);
            var nrElem = (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

            var info = manager.GetRawdataInfo(rawInfoOID);
            if (info == null) return list;

            var arrayData = new byte[info.Length];
            File.Read(info.Position, arrayData);

            var currentIndex = 0;
            var elemBytes = new byte[info.ElementLength];
            for (var i = 0; i < nrElem; i++)
            {
                Array.Copy(arrayData, currentIndex, elemBytes, 0, info.ElementLength);
                currentIndex += info.ElementLength;

                if (objSerializer != null) //complex object
                {
                    var kv = objSerializer.ReadOIDAndTID(elemBytes);
                    list.Add(kv);
                }
            }

            return list;
        }
#if ASYNC
        public async Task<List<KeyValuePair<int, int>>> ReadComplexArrayOidsAsync(byte[] bytes, int dbVersion,
            ObjectSerializer objSerializer)
        {
            var list = new List<KeyValuePair<int, int>>();

            if (bytes[0] == 1) //is null
                return list;

            var oidBytes = new byte[4];
            Array.Copy(bytes, 1, oidBytes, 0, 4);
            var rawInfoOID = (int)ByteConverter.DeserializeValueType(typeof(int), oidBytes, dbVersion);

            var nrElemeBytes = new byte[4];
            Array.Copy(bytes, MetaExtractor.ExtraSizeForArray - 4, nrElemeBytes, 0, 4);
            var nrElem = (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

            var info = await manager.GetRawdataInfoAsync(rawInfoOID).ConfigureAwait(false);
            if (info == null) return list;

            var arrayData = new byte[info.Length];
            await File.ReadAsync(info.Position, arrayData).ConfigureAwait(false);

            var currentIndex = 0;
            var elemBytes = new byte[info.ElementLength];
            for (var i = 0; i < nrElem; i++)
            {
                Array.Copy(arrayData, currentIndex, elemBytes, 0, info.ElementLength);
                currentIndex += info.ElementLength;

                if (objSerializer != null) //complex object
                {
                    var kv = objSerializer.ReadOIDAndTID(elemBytes);
                    list.Add(kv);
                }
            }

            return list;
        }
#endif
        public int ReadComplexArrayFirstTID(byte[] bytes, int dbVersion, ObjectSerializer objSerializer)
        {
            if (bytes[0] == 1) //is null
                return -1;

            var oidBytes = new byte[4];
            Array.Copy(bytes, 1, oidBytes, 0, 4);
            var rawInfoOID = (int)ByteConverter.DeserializeValueType(typeof(int), oidBytes, dbVersion);

            var nrElemeBytes = new byte[4];
            Array.Copy(bytes, MetaExtractor.ExtraSizeForArray - 4, nrElemeBytes, 0, 4);
            var nrElem = (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

            var info = manager.GetRawdataInfo(rawInfoOID);
            if (info == null) return -1;

            var arrayData = new byte[info.Length];
            File.Read(info.Position, arrayData);

            var currentIndex = 0;
            var elemBytes = new byte[info.ElementLength];
            for (var i = 0; i < nrElem; i++)
            {
                Array.Copy(arrayData, currentIndex, elemBytes, 0, info.ElementLength);
                currentIndex += info.ElementLength;

                if (objSerializer != null) //complex object
                {
                    var kv = objSerializer.ReadOIDAndTID(elemBytes);
                    if (kv.Value > 0) return kv.Value;
                }
            }

            return -1;
        }
#if ASYNC
        public async Task<int> ReadComplexArrayFirstTIDAsync(byte[] bytes, int dbVersion,
            ObjectSerializer objSerializer)
        {
            if (bytes[0] == 1) //is null
                return -1;

            var oidBytes = new byte[4];
            Array.Copy(bytes, 1, oidBytes, 0, 4);
            var rawInfoOID = (int)ByteConverter.DeserializeValueType(typeof(int), oidBytes, dbVersion);

            var nrElemeBytes = new byte[4];
            Array.Copy(bytes, MetaExtractor.ExtraSizeForArray - 4, nrElemeBytes, 0, 4);
            var nrElem = (int)ByteConverter.DeserializeValueType(typeof(int), nrElemeBytes, dbVersion);

            var info = await manager.GetRawdataInfoAsync(rawInfoOID).ConfigureAwait(false);
            if (info == null) return -1;

            var arrayData = new byte[info.Length];
            await File.ReadAsync(info.Position, arrayData).ConfigureAwait(false);

            var currentIndex = 0;
            var elemBytes = new byte[info.ElementLength];
            for (var i = 0; i < nrElem; i++)
            {
                Array.Copy(arrayData, currentIndex, elemBytes, 0, info.ElementLength);
                currentIndex += info.ElementLength;

                if (objSerializer != null) //complex object
                {
                    var kv = objSerializer.ReadOIDAndTID(elemBytes);
                    if (kv.Value > 0) return kv.Value;
                }
            }

            return -1;
        }
#endif
        internal void TransactionCommitStatus(bool started)
        {
            _transactionCommitStarted = true;
        }

        internal void Flush()
        {
            File.Flush();
        }
#if ASYNC
        internal async Task FlushAsync()
        {
            await File.FlushAsync().ConfigureAwait(false);
        }
#endif
        internal void Close()
        {
            Flush();
            if (filesCache.ContainsKey(storageEngine.path))
            {
                filesCache.Remove(storageEngine.path);
                file.Close();
                file = null;
            }
        }
#if ASYNC
        internal async Task CloseAsync()
        {
            await FlushAsync().ConfigureAwait(false);
            if (filesCache.ContainsKey(storageEngine.path))
            {
                filesCache.Remove(storageEngine.path);
                file.Close();
                file = null;
            }
        }
#endif
        internal void MarkRawInfoAsFree(int oid)
        {
            manager.MarkRawInfoAsFree(oid);
        }
#if ASYNC
        internal async Task MarkRawInfoAsFreeAsync(int oid)
        {
            await manager.MarkRawInfoAsFreeAsync(oid).ConfigureAwait(false);
        }
#endif

        internal object DeserializeTextArray(Type type, byte[] bytes, bool p1, int p2)
        {
            throw new NotImplementedException();
        }
    }
}