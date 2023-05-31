using System.Threading.Tasks;

namespace sqoDB.Core
{
    internal interface ISqoFile
    {
        bool IsClosed { get; }

        long Length { get; set; }

        void Write(long pos, byte[] buf);

        void Write(byte[] buf);
        int Read(long pos, byte[] buf);

        void Flush();


        void Close();


#if ASYNC
        Task WriteAsync(long pos, byte[] buf);
        Task WriteAsync(byte[] buf);
        Task<int> ReadAsync(long pos, byte[] buf);
        Task FlushAsync();
#endif
    }
}