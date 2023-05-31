using sqoDB.Meta;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Core
{
    internal class ComplexTypeTransformer : IByteTransformer
    {
        private readonly FieldSqoInfo fi;
        private readonly ObjectSerializer serializer;
        private readonly SqoTypeInfo ti;

        public ComplexTypeTransformer(ObjectSerializer serializer, SqoTypeInfo ti, FieldSqoInfo fi)
        {
            this.serializer = serializer;
            this.fi = fi;
            this.ti = ti;
        }


        #region IByteTransformer Members

        public byte[] GetBytes(object obj)
        {
            return serializer.GetComplexObjectBytes(obj);
        }

        public object GetObject(byte[] bytes)
        {
            return serializer.ReadComplexObject(bytes, ti.Type, fi.Name);
        }
#if ASYNC
        public async Task<byte[]> GetBytesAsync(object obj)
        {
            return await serializer.GetComplexObjectBytesAsync(obj).ConfigureAwait(false);
        }

        public async Task<object> GetObjectAsync(byte[] bytes)
        {
            return await serializer.ReadComplexObjectAsync(bytes, ti.Type, fi.Name).ConfigureAwait(false);
        }
#endif

        #endregion
    }
}