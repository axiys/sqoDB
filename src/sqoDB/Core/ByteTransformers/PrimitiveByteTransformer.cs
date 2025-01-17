﻿using sqoDB.Meta;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Core
{
    internal class PrimitiveByteTransformer : IByteTransformer
    {
        private readonly FieldSqoInfo fi;
        private readonly SqoTypeInfo ti;

        public PrimitiveByteTransformer(FieldSqoInfo fi, SqoTypeInfo ti)
        {
            this.fi = fi;
            this.ti = ti;
        }

        #region IByteTransformer Members

        public byte[] GetBytes(object obj)
        {
            return ByteConverter.SerializeValueType(obj, fi.AttributeType, fi.Header.Length, fi.Header.RealLength,
                ti.Header.version);
        }

        public object GetObject(byte[] bytes)
        {
            return ByteConverter.DeserializeValueType(fi.AttributeType, bytes, true, ti.Header.version);
        }


#if ASYNC
        public async Task<byte[]> GetBytesAsync(object obj)
        {
            return ByteConverter.SerializeValueType(obj, fi.AttributeType, fi.Header.Length, fi.Header.RealLength,
                ti.Header.version);
        }

        public async Task<object> GetObjectAsync(byte[] bytes)
        {
            return ByteConverter.DeserializeValueType(fi.AttributeType, bytes, true, ti.Header.version);
        }
#endif

        #endregion
    }
}