#if WinRT
using Windows.Security.Cryptography;
using Windows.Storage.Streams;
using Windows.Security.Cryptography.Core;
#else
using System.Security.Cryptography;
#endif
using System;
using System.IO;
using System.Text;

namespace sqoDB.Utilities
{
    internal class Decryptor
    {
#if WinRT
        public static string DecryptRJ128(string prm_key, string prm_iv, string prm_text_to_decrypt)
        {
            IBuffer encrypted;
            IBuffer buffer;
            IBuffer iv = null;
            byte[] keyBuff = System.Text.Encoding.UTF8.GetBytes(prm_key);
            byte[] IVBuff = System.Text.Encoding.UTF8.GetBytes(prm_iv);
            
            SymmetricKeyAlgorithmProvider algorithm =
 SymmetricKeyAlgorithmProvider.OpenAlgorithm("AES_CBC_PKCS7"); //This is the only one using two fixed keys and variable block size

            IBuffer keymaterial =
 CryptographicBuffer.CreateFromByteArray(keyBuff); // as said..I have fixed keys (see above)
            CryptographicKey key = algorithm.CreateSymmetricKey(keymaterial);
            
            byte[] sEncrypted = Convert.FromBase64String(prm_text_to_decrypt);
            
            iv = CryptographicBuffer.CreateFromByteArray(IVBuff); // again my IV is fixed
            buffer = CryptographicBuffer.CreateFromByteArray(sEncrypted);  //Directly converting GUID to byte array
            encrypted = Windows.Security.Cryptography.Core.CryptographicEngine.Decrypt(key, buffer, iv);

            return CryptographicBuffer.ConvertBinaryToString(BinaryStringEncoding.Utf8,encrypted);
        }
#else
        public static string DecryptRJ128(string prm_key, string prm_iv, string prm_text_to_decrypt)
        {
            var sEncryptedString = prm_text_to_decrypt;
#if CF
            RijndaelManaged myRijndael = new RijndaelManaged();
#else
            var myRijndael = new AesManaged();
#endif
            myRijndael.KeySize = 128;
            myRijndael.BlockSize = 128;
            var key = Encoding.UTF8.GetBytes(prm_key);
            var IV = Encoding.UTF8.GetBytes(prm_iv);
            var decryptor = myRijndael.CreateDecryptor(key, IV);
            var sEncrypted = Convert.FromBase64String(sEncryptedString);
            var fromEncrypt = new byte[sEncrypted.Length];
            var msDecrypt = new MemoryStream(sEncrypted);
            var csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read);
            csDecrypt.Read(fromEncrypt, 0, fromEncrypt.Length);

            return Encoding.UTF8.GetString(fromEncrypt, 0, fromEncrypt.Length).TrimEnd('\0');
        }
        //http://stackoverflow.com/questions/224453/decrypt-php-encrypted-string-in-c

#endif
    }
}