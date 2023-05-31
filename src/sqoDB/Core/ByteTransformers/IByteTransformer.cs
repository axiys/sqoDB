#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Core
{
    internal interface IByteTransformer
    {
        byte[] GetBytes(object obj);
        object GetObject(byte[] bytes);
#if ASYNC
        Task<byte[]> GetBytesAsync(object obj);
        Task<object> GetObjectAsync(byte[] bytes);
#endif
    }
}