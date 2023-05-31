using System.Collections.Generic;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Core
{
    internal class SerializerFactory
    {
        private static readonly Dictionary<string, ObjectSerializer> serializers =
            new Dictionary<string, ObjectSerializer>();

        private static readonly object _syncRoot = new object();

        public static ObjectSerializer GetSerializer(string folderPath, string typeName, bool useElevatedTrust)
        {
            return GetSerializer(folderPath, typeName, useElevatedTrust, "sqo");
        }

        public static ObjectSerializer GetSerializer(string folderPath, string typeName, bool useElevatedTrust,
            string fileExtension)
        {
            string fileFull = null;
            if (SiaqodbConfigurator.EncryptedDatabase)
                fileFull = folderPath + Path.DirectorySeparatorChar + typeName + ".e" + fileExtension;
            else
                fileFull = folderPath + Path.DirectorySeparatorChar + typeName + "." + fileExtension;
            lock (_syncRoot)
            {
                if (serializers.ContainsKey(fileFull))
                {
                    if (serializers[fileFull].IsClosed) serializers[fileFull].Open(useElevatedTrust);

                    return serializers[fileFull];
                }

                var ser = new ObjectSerializer(fileFull, useElevatedTrust);
                serializers[fileFull] = ser;
                return ser;
            }
        }
#if WinRT
#endif
        public static void CloseAll()
        {
            lock (_syncRoot)
            {
                foreach (var key in serializers.Keys) serializers[key].Close();
                serializers.Clear();
            }
        }
#if ASYNC
        public static async Task CloseAllAsync()
        {
            foreach (var key in serializers.Keys) await serializers[key].CloseAsync().ConfigureAwait(false);
        }

#endif
        public static void FlushAll()
        {
            lock (_syncRoot)
            {
                foreach (var key in serializers.Keys) serializers[key].Flush();
            }
        }
#if ASYNC
        public static async Task FlushAllAsync()
        {
            foreach (var key in serializers.Keys) await serializers[key].FlushAsync().ConfigureAwait(false);
        }

#endif
        public static void ClearCache(string folderName)
        {
            lock (_syncRoot)
            {
                var keysToBeRemoved = new List<string>();
                foreach (var key in serializers.Keys)
                    if (Path.GetDirectoryName(key).TrimEnd(Path.DirectorySeparatorChar) ==
                        folderName.TrimEnd(Path.DirectorySeparatorChar))
                    {
                        serializers[key].Close();
                        keysToBeRemoved.Add(key);
                    }

                foreach (var k in keysToBeRemoved) serializers.Remove(k);
            }
        }
    }
}