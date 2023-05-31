namespace sqoDB.Encryption
{
    public interface IEncryptor
    {
        void Encrypt(byte[] bytes, int off, int len);
        void Decrypt(byte[] bytes, int off, int len);
        int GetBlockSize();
    }
}