using System.Collections;
using System.Threading.Tasks;
using sqoDB.Exceptions;
using sqoDB.Meta;
using sqoDB.MetaObjects;

namespace sqoDB.Core
{
    internal class DictionaryByteTransformer : IByteTransformer
    {
        private readonly FieldSqoInfo fi;
        private readonly int parentOID;
        private readonly RawdataSerializer rawSerializer;
        private readonly ObjectSerializer serializer;
        private readonly SqoTypeInfo ti;

        public DictionaryByteTransformer(ObjectSerializer serializer, RawdataSerializer rawSerializer, SqoTypeInfo ti,
            FieldSqoInfo fi, int parentOID)
        {
            this.serializer = serializer;
            this.rawSerializer = rawSerializer;
            this.ti = ti;
            this.fi = fi;
            this.parentOID = parentOID;
        }

        #region IByteTransformer Members

        public byte[] GetBytes(object obj)
        {
            DictionaryInfo dInfo = null;

            if (parentOID > 0)
            {
                dInfo = serializer.GetDictInfoOfField(ti, parentOID, fi);
            }
            else
            {
                if (obj != null)
                {
                    var actualDict = (IDictionary)obj;
                    var keyValueType = actualDict.GetType().GetGenericArguments();
                    if (keyValueType.Length != 2)
                        throw new NotSupportedTypeException("Type:" + actualDict.GetType() + " is not supported");
                    var keyTypeId = MetaExtractor.GetAttributeType(keyValueType[0]);
                    var valueTypeId = MetaExtractor.GetAttributeType(keyValueType[1]);
                    dInfo = new DictionaryInfo();
                    dInfo.KeyTypeId = keyTypeId;
                    dInfo.ValueTypeId = valueTypeId;
                }
            }

            return rawSerializer.SerializeDictionary(obj, fi.Header.Length, ti.Header.version, dInfo, serializer);
        }

        public object GetObject(byte[] bytes)
        {
            return rawSerializer.DeserializeDictionary(fi.AttributeType, bytes, ti.Header.version, serializer, ti.Type,
                fi.Name);
        }

        #endregion

#if ASYNC
        public async Task<byte[]> GetBytesAsync(object obj)
        {
            DictionaryInfo dInfo = null;

            if (parentOID > 0)
            {
                dInfo = await serializer.GetDictInfoOfFieldAsync(ti, parentOID, fi).ConfigureAwait(false);
            }
            else
            {
                if (obj != null)
                {
                    var actualDict = (IDictionary)obj;
                    var keyValueType = actualDict.GetType().GetGenericArguments();
                    if (keyValueType.Length != 2)
                        throw new NotSupportedTypeException("Type:" + actualDict.GetType() + " is not supported");
                    var keyTypeId = MetaExtractor.GetAttributeType(keyValueType[0]);
                    var valueTypeId = MetaExtractor.GetAttributeType(keyValueType[1]);
                    dInfo = new DictionaryInfo();
                    dInfo.KeyTypeId = keyTypeId;
                    dInfo.ValueTypeId = valueTypeId;
                }
            }

            return await rawSerializer
                .SerializeDictionaryAsync(obj, fi.Header.Length, ti.Header.version, dInfo, serializer)
                .ConfigureAwait(false);
        }

        public async Task<object> GetObjectAsync(byte[] bytes)
        {
            return await rawSerializer
                .DeserializeDictionaryAsync(fi.AttributeType, bytes, ti.Header.version, serializer, ti.Type, fi.Name)
                .ConfigureAwait(false);
        }
#endif
    }
}