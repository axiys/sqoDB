using sqoDB.Meta;
using sqoDB.Utilities;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Core
{
    internal class ArrayByteTranformer : IByteTransformer
    {
        private readonly FieldSqoInfo fi;
        private readonly int parentOID;
        private readonly RawdataSerializer rawSerializer;
        private readonly ObjectSerializer serializer;
        private readonly SqoTypeInfo ti;

        public ArrayByteTranformer(ObjectSerializer serializer, RawdataSerializer rawSerializer, SqoTypeInfo ti,
            FieldSqoInfo fi, int parentOID)
        {
            this.serializer = serializer;
            this.ti = ti;
            this.fi = fi;
            this.rawSerializer = rawSerializer;
            this.parentOID = parentOID;
        }

        #region IByteTransformer Members

        public byte[] GetBytes(object obj)
        {
            ATuple<int, int> arrayMeta = null;
            if (parentOID > 0) //means already exists the rawOID
                arrayMeta = serializer.GetArrayMetaOfField(ti, parentOID, fi);
            if (fi.AttributeTypeId == MetaExtractor.ArrayTypeIDExtra + MetaExtractor.textID)
                return rawSerializer.SerializeArray(obj, fi.AttributeType, fi.Header.Length, fi.Header.RealLength,
                    ti.Header.version, arrayMeta, serializer, true);
            return rawSerializer.SerializeArray(obj, fi.AttributeType, fi.Header.Length, fi.Header.RealLength,
                ti.Header.version, arrayMeta, serializer, false);
        }

        public object GetObject(byte[] bytes)
        {
            object fieldVal = null;
            if (fi.AttributeTypeId == MetaExtractor.ArrayTypeIDExtra + MetaExtractor.complexID ||
                fi.AttributeTypeId ==
                MetaExtractor.ArrayTypeIDExtra + MetaExtractor.jaggedArrayID) // array of complexType
                fieldVal = rawSerializer.DeserializeArray(fi.AttributeType, bytes, true, ti.Header.version, fi.IsText,
                    false, serializer, ti.Type, fi.Name);
            else if (fi.AttributeTypeId == MetaExtractor.ArrayTypeIDExtra + MetaExtractor.textID)
                fieldVal = rawSerializer.DeserializeArray(fi.AttributeType, bytes, true, ti.Header.version, false,
                    true);
            else
                fieldVal = rawSerializer.DeserializeArray(fi.AttributeType, bytes, true, ti.Header.version, fi.IsText,
                    false);
            return fieldVal;
        }


#if ASYNC
        public async Task<byte[]> GetBytesAsync(object obj)
        {
            ATuple<int, int> arrayMeta = null;
            if (parentOID > 0) //means already exists the rawOID
                arrayMeta = await serializer.GetArrayMetaOfFieldAsync(ti, parentOID, fi).ConfigureAwait(false);
            if (fi.AttributeTypeId == MetaExtractor.ArrayTypeIDExtra + MetaExtractor.textID)
                return await rawSerializer.SerializeArrayAsync(obj, fi.AttributeType, fi.Header.Length,
                    fi.Header.RealLength, ti.Header.version, arrayMeta, serializer, true).ConfigureAwait(false);
            return await rawSerializer.SerializeArrayAsync(obj, fi.AttributeType, fi.Header.Length,
                fi.Header.RealLength, ti.Header.version, arrayMeta, serializer, false).ConfigureAwait(false);
        }

        public async Task<object> GetObjectAsync(byte[] bytes)
        {
            object fieldVal = null;
            if (fi.AttributeTypeId == MetaExtractor.ArrayTypeIDExtra + MetaExtractor.complexID ||
                fi.AttributeTypeId ==
                MetaExtractor.ArrayTypeIDExtra + MetaExtractor.jaggedArrayID) // array of complexType
                fieldVal = await rawSerializer.DeserializeArrayAsync(fi.AttributeType, bytes, true, ti.Header.version,
                    fi.IsText, false, serializer, ti.Type, fi.Name).ConfigureAwait(false);
            else if (fi.AttributeTypeId == MetaExtractor.ArrayTypeIDExtra + MetaExtractor.textID)
                fieldVal = await rawSerializer
                    .DeserializeArrayAsync(fi.AttributeType, bytes, true, ti.Header.version, false, true)
                    .ConfigureAwait(false);
            else
                fieldVal = await rawSerializer
                    .DeserializeArrayAsync(fi.AttributeType, bytes, true, ti.Header.version, fi.IsText, false)
                    .ConfigureAwait(false);
            return fieldVal;
        }
#endif

        #endregion
    }
}