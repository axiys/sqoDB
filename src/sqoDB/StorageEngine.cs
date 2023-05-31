using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using sqoDB.Cache;
using sqoDB.Core;
using sqoDB.Exceptions;
using sqoDB.Indexes;
using sqoDB.Meta;
using sqoDB.MetaObjects;
using sqoDB.Transactions;
using sqoDB.Utilities;
#if ASYNC
using System.Threading.Tasks;

#endif

#if WinRT
using Windows.Storage.Search;
using Windows.Storage;
#endif
#if SILVERLIGHT
	using System.IO.IsolatedStorage;
#endif


namespace sqoDB
{
    [Obfuscation(Feature = "Apply to member * when event: renaming", Exclude = true)]
    internal partial class StorageEngine
    {
        #region VAR DECLARATIONS

#if WinRT
        internal StorageFolder storageFolder;
#endif
        internal string path;
        private readonly object _syncRoot = new object();
        internal MetaCache metaCache;

        private readonly RawdataSerializer rawSerializer;
        internal IndexManager indexManager;
        private readonly CircularRefCache circularRefCache = new CircularRefCache();
        private List<ATuple<Type, string>> includePropertiesCache;
        private List<object> parentsComparison;
        private readonly bool useElevatedTrust;

#if UNITY3D
        private EventHandler<LoadingObjectEventArgs> loadingObject;
        public event EventHandler<LoadingObjectEventArgs> LoadingObject
        {
            add
            {
                lock (_syncRoot)
                {
                    loadingObject += value;
                }
            }
            remove
            {
                lock (_syncRoot)
                {
                    loadingObject -= value;
                }
            }

        }
        private EventHandler<LoadedObjectEventArgs> loadedObject;
        public event EventHandler<LoadedObjectEventArgs> LoadedObject
        {
            add
            {
                lock (_syncRoot)
                {
                    loadedObject += value;
                }
            }
            remove
            {
                lock (_syncRoot)
                {
                    loadedObject -= value;
                }
            }

        }
        private EventHandler<ComplexObjectEventArgs> needSaveComplexObject;
        public event EventHandler<ComplexObjectEventArgs> NeedSaveComplexObject
        {
            add
            {
                lock (_syncRoot)
                {
                    needSaveComplexObject += value;
                }
            }
            remove
            {
                lock (_syncRoot)
                {
                    needSaveComplexObject -= value;
                }
            }

        }
		#if ASYNC
		private ComplexObjectEventHandler needSaveComplexObjectAsync;
		public event ComplexObjectEventHandler NeedSaveComplexObjectAsync
		{
			add
			{
				lock (_syncRoot)
				{
					needSaveComplexObjectAsync += value;
				}
			}
			remove
			{
				lock (_syncRoot)
				{
					needSaveComplexObjectAsync -= value;
				}
			}

		}
#endif
#else
        public event EventHandler<LoadingObjectEventArgs> LoadingObject;
        public event EventHandler<LoadedObjectEventArgs> LoadedObject;
        public event EventHandler<ComplexObjectEventArgs> NeedSaveComplexObject;
#if ASYNC
        public event ComplexObjectEventHandler NeedSaveComplexObjectAsync;
#endif

#endif
#if UNITY3D || CF || MONODROID
#else
        public event EventHandler<IndexesSaveAsyncFinishedArgs> IndexesSaveAsyncFinished;
#endif

        #endregion

        #region CTOR

        public StorageEngine(string path)
        {
            this.path = path;
            SerializerFactory.ClearCache(path);
            rawSerializer = new RawdataSerializer(this, useElevatedTrust);
        }

        public StorageEngine(string path, bool useElevatedTrust)
        {
            this.path = path;
            SerializerFactory.ClearCache(path);
            this.useElevatedTrust = useElevatedTrust;
            rawSerializer = new RawdataSerializer(this, useElevatedTrust);
        }

#if WinRT
        public StorageEngine(StorageFolder s)
        {

            if (!SqoLicense.LicenseValid())
            {
                  throw new InvalidLicenseException("License not valid!");
            }
            this.storageFolder = s;
            this.path = storageFolder.Path;
            SerializerFactory.ClearCache(this.storageFolder.Path);
            this.rawSerializer = new RawdataSerializer(this, useElevatedTrust);


        }
#endif

        #endregion

        #region TYPE MANAGEMENT

        public void SaveType(SqoTypeInfo ti)
        {
            if (ti.Header.TID == 0) ti.Header.TID = metaCache.GetNextTID();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            serializer.SerializeType(ti);
        }
#if ASYNC
        public async Task SaveTypeAsync(SqoTypeInfo ti)
        {
            if (ti.Header.TID == 0) ti.Header.TID = metaCache.GetNextTID();
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            await serializer.SerializeTypeAsync(ti).ConfigureAwait(false);
        }
#endif
        private string GetFileByType(SqoTypeInfo ti)
        {
            if (ti.Header.version > -31 && ti.Type != null) //version less than 3.1
                return MetaHelper.GetOldFileNameByType(ti.Type);
            return GetFileByType(ti.TypeName);
        }

        private string GetFileByType(string typeName)
        {
            var customName = CacheCustomFileNames.GetFileName(typeName);
            if (customName != null) return customName;

            var assemblyName = typeName.Substring(typeName.LastIndexOf(',') + 1);
            var onlyTypeName = typeName.Substring(0, typeName.LastIndexOf(','));
            var fileName = onlyTypeName + "." + assemblyName;

            //fileName = fileName.GetHashCode().ToString();

#if SILVERLIGHT
            if (!SiaqodbConfigurator.UseLongDBFileNames && !fileName.StartsWith("sqoDB.Indexes.BTreeNode"))
            {
                fileName = fileName.GetHashCode().ToString();
            }
#endif

            return fileName;
        }

        internal SqoTypeInfo GetSqoTypeInfoSoft(Type t)
        {
            SqoTypeInfo ti = null;
            var objType = t;
            if (metaCache.Contains(objType))
                ti = metaCache.GetSqoTypeInfo(objType);
            else
                ti = MetaExtractor.GetSqoTypeInfo(objType);
            return ti;
        }
#if !WinRT
        internal void LoadMetaDataTypesForManager()
        {
            var rawdatainfoName = MetaHelper.GetOldFileNameByType(typeof(RawdataInfo));
            if (MetaHelper.FileExists(path, rawdatainfoName, useElevatedTrust))
            {
                UpgradeInternalSqoTypeInfos(typeof(RawdataInfo), "rawdatainfo", false);
            }
            else
            {
                CacheCustomFileNames.AddFileNameForType(new SqoTypeInfo(typeof(RawdataInfo)).TypeName, "rawdatainfo",
                    false);
                var seralizer = SerializerFactory.GetSerializer(path,
                    GetFileByType(new SqoTypeInfo(typeof(RawdataInfo)).TypeName), useElevatedTrust);
                var ti = seralizer.DeserializeSqoTypeInfo(true);
                if (ti != null) CompareSchema(ti);
            }
        }
#if ASYNC
        internal async Task LoadMetaDataTypesForManagerAsync()
        {
            var rawdatainfoName = MetaHelper.GetOldFileNameByType(typeof(RawdataInfo));
            if (MetaHelper.FileExists(path, rawdatainfoName, useElevatedTrust))
            {
                await UpgradeInternalSqoTypeInfosAsync(typeof(RawdataInfo), "rawdatainfo", false).ConfigureAwait(false);
            }
            else
            {
                CacheCustomFileNames.AddFileNameForType(new SqoTypeInfo(typeof(RawdataInfo)).TypeName, "rawdatainfo",
                    false);
                var seralizer = SerializerFactory.GetSerializer(path,
                    GetFileByType(new SqoTypeInfo(typeof(RawdataInfo)).TypeName), useElevatedTrust);
                var ti = await seralizer.DeserializeSqoTypeInfoAsync(true).ConfigureAwait(false);
                if (ti != null) await CompareSchemaAsync(ti).ConfigureAwait(false);
            }
        }
#endif

#endif

        internal void LoadMetaDataTypes()
        {
            var rawdatainfoName = MetaHelper.GetOldFileNameByType(typeof(RawdataInfo));

#if !WinRT
            if (MetaHelper.FileExists(path, rawdatainfoName, useElevatedTrust))
            {
                UpgradeInternalSqoTypeInfos(typeof(RawdataInfo), "rawdatainfo", false);
                var indexinfoName = MetaHelper.GetOldFileNameByType(typeof(IndexInfo2));
                if (MetaHelper.FileExists(path, indexinfoName, useElevatedTrust))
                    UpgradeInternalSqoTypeInfos(typeof(IndexInfo2), "indexinfo2", true);
            }

            else
#endif
            {
                CacheCustomFileNames.AddFileNameForType(new SqoTypeInfo(typeof(RawdataInfo)).TypeName, "rawdatainfo",
                    false);
                CacheCustomFileNames.AddFileNameForType(new SqoTypeInfo(typeof(IndexInfo2)).TypeName, "indexinfo2",
                    false);
#if KEVAST
                CacheCustomFileNames.AddFileNameForType(new SqoTypeInfo(typeof(KeVaSt.KVSInfo)).TypeName, "KVSInfo", false);
#endif
                var seralizer = SerializerFactory.GetSerializer(path,
                    GetFileByType(new SqoTypeInfo(typeof(RawdataInfo)).TypeName), useElevatedTrust);
                var ti = seralizer.DeserializeSqoTypeInfo(true);
                if (ti != null) CompareSchema(ti);


                seralizer = SerializerFactory.GetSerializer(path,
                    GetFileByType(new SqoTypeInfo(typeof(IndexInfo2)).TypeName), useElevatedTrust);
                ti = seralizer.DeserializeSqoTypeInfo(true);
                if (ti != null) CompareSchema(ti);
#if KEVAST
                seralizer =
 SerializerFactory.GetSerializer(path, this.GetFileByType(new SqoTypeInfo(typeof(KeVaSt.KVSInfo)).TypeName), this.useElevatedTrust);
                ti = seralizer.DeserializeSqoTypeInfo(true);
                if (ti != null)
                {
                    this.CompareSchema(ti);
                }
#endif
            }
        }

#if ASYNC
        internal async Task LoadMetaDataTypesAsync()
        {
            var rawdatainfoName = MetaHelper.GetOldFileNameByType(typeof(RawdataInfo));

#if !WinRT
            if (MetaHelper.FileExists(path, rawdatainfoName, useElevatedTrust))
            {
                await UpgradeInternalSqoTypeInfosAsync(typeof(RawdataInfo), "rawdatainfo", false).ConfigureAwait(false);
                var indexinfoName = MetaHelper.GetOldFileNameByType(typeof(IndexInfo2));
                if (MetaHelper.FileExists(path, indexinfoName, useElevatedTrust))
                    await UpgradeInternalSqoTypeInfosAsync(typeof(IndexInfo2), "indexinfo2", true)
                        .ConfigureAwait(false);
            }

            else
#endif
            {
                CacheCustomFileNames.AddFileNameForType(new SqoTypeInfo(typeof(RawdataInfo)).TypeName, "rawdatainfo",
                    false);
                CacheCustomFileNames.AddFileNameForType(new SqoTypeInfo(typeof(IndexInfo2)).TypeName, "indexinfo2",
                    false);
#if KEVAST
                CacheCustomFileNames.AddFileNameForType(new SqoTypeInfo(typeof(KeVaSt.KVSInfo)).TypeName, "KVSInfo", false);
#endif
                var seralizer = SerializerFactory.GetSerializer(path,
                    GetFileByType(new SqoTypeInfo(typeof(RawdataInfo)).TypeName), useElevatedTrust);
                var ti = await seralizer.DeserializeSqoTypeInfoAsync(true).ConfigureAwait(false);
                if (ti != null) await CompareSchemaAsync(ti).ConfigureAwait(false);


                seralizer = SerializerFactory.GetSerializer(path,
                    GetFileByType(new SqoTypeInfo(typeof(IndexInfo2)).TypeName), useElevatedTrust);
                ti = await seralizer.DeserializeSqoTypeInfoAsync(true).ConfigureAwait(false);
                if (ti != null) await CompareSchemaAsync(ti).ConfigureAwait(false);
#if KEVAST
                seralizer =
 SerializerFactory.GetSerializer(path, this.GetFileByType(new SqoTypeInfo(typeof(KeVaSt.KVSInfo)).TypeName), this.useElevatedTrust);
                ti = await seralizer.DeserializeSqoTypeInfoAsync(true).ConfigureAwait(false);
                if (ti != null)
                {
                    await this.CompareSchemaAsync(ti).ConfigureAwait(false);
                }
#endif
            }
        }

#endif

        internal List<SqoTypeInfo> LoadAllTypesForObjectManager()
        {
            var extension = ".sqo";
            if (SiaqodbConfigurator.EncryptedDatabase) extension = ".esqo";
#if SILVERLIGHT
            if (!this.useElevatedTrust)
            {
                List<SqoTypeInfo> list = new List<SqoTypeInfo>();
                IsolatedStorageFile isf = IsolatedStorageFile.GetUserStoreForApplication();

                if (isf.DirectoryExists(path))
                {

                }
                else
                {
                    isf.CreateDirectory(path);
                }
               
                string searchPath = Path.Combine(path, "*"+extension);
                string[] files = isf.GetFileNames(searchPath);

                foreach (string f in files)
                {
                    string typeName = f.Replace(extension, "");
             if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo") || typeName.StartsWith("sqoDB.Indexes.BTreeNode"))//engine types
                {
                    continue;
                }
                    ObjectSerializer seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);
                    SqoTypeInfo ti = seralizer.DeserializeSqoTypeInfo(false);
                    if (ti != null)
                    {
                        list.Add(ti);
                    }
                }
                return list;
            }
            else //elevatedTrust
            { 
#if SL4
             if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }


               

                List<SqoTypeInfo> list = new List<SqoTypeInfo>();

                System.IO.DirectoryInfo di = new System.IO.DirectoryInfo(path);
                foreach (FileInfo f in di.EnumerateFiles("*"+extension))
                {
                    string typeName = f.Name.Replace(extension, "");
                    if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo") || typeName.StartsWith("sqoDB.Indexes.BTreeNode"))//engine types
                {
                    continue;
                }
                    ObjectSerializer seralizer = SerializerFactory.GetSerializer(path, typeName,useElevatedTrust);

                    SqoTypeInfo ti = seralizer.DeserializeSqoTypeInfo(false);
                    if (ti != null)
                    {
                        list.Add(ti);
                    }
                }
                return list;
#else
                return null; // will never be here
#endif

            }
#elif WinRT
            //List<string> fileFilter = new List<string>();
          //  fileFilter.Add("*");
          //  QueryOptions qo = new QueryOptions();
            //qo.se = fileFilter;
           // qo.UserSearchFilter = extension;
            //StorageFileQueryResult resultQuery = storageFolder.CreateFileQueryWithOptions(qo);
            IReadOnlyList<IStorageFile> files = storageFolder.GetFilesAsync().AsTask().Result;

            List<SqoTypeInfo> list = new List<SqoTypeInfo>();


            foreach (IStorageFile f in files)
            {
                if (f.FileType != extension)
                    continue;
                string typeName = f.Name.Replace(extension, "");
                if (typeName.StartsWith("sqoDB.Indexes.IndexInfo2.") || typeName.StartsWith("sqoDB.MetaObjects.RawdataInfo."))//engine types
                {
                    continue;
                }
                ObjectSerializer seralizer =
 SerializerFactory.GetSerializer(storageFolder.Path, typeName, useElevatedTrust);

                SqoTypeInfo ti = seralizer.DeserializeSqoTypeInfo(false);
                if (ti != null)
                {
                    ti.FileNameForManager = typeName;
                    list.Add(ti);
                }
            }
            return list;
#else
            var list = new List<SqoTypeInfo>();

            var di = new DirectoryInfo(path);
            //TODO: throw exception
            var fi = di.GetFiles("*" + extension);

            foreach (var f in fi)
            {
                var typeName = f.Name.Replace(extension, "");
                if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo") ||
                    typeName.StartsWith("sqoDB.Indexes.BTreeNode")) //engine types
                    continue;
                var seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);

                var ti = seralizer.DeserializeSqoTypeInfo(false);
                if (ti != null && !ti.TypeName.StartsWith("sqoDB.Indexes.BTreeNode"))
                {
                    ti.FileNameForManager = typeName;
                    list.Add(ti);
                }
            }

            return list;
#endif
        }

#if ASYNC
        internal async Task<List<SqoTypeInfo>> LoadAllTypesForObjectManagerAsync()
        {
            var extension = ".sqo";
            if (SiaqodbConfigurator.EncryptedDatabase) extension = ".esqo";
#if SILVERLIGHT
            if (!this.useElevatedTrust)
            {
                List<SqoTypeInfo> list = new List<SqoTypeInfo>();
                IsolatedStorageFile isf = IsolatedStorageFile.GetUserStoreForApplication();

                if (isf.DirectoryExists(path))
                {

                }
                else
                {
                    isf.CreateDirectory(path);
                }
               
                string searchPath = Path.Combine(path, "*"+extension);
                string[] files = isf.GetFileNames(searchPath);

                foreach (string f in files)
                {
                    string typeName = f.Replace(extension, "");
             if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo") || typeName.StartsWith("sqoDB.Indexes.BTreeNode"))//engine types
                {
                    continue;
                }
                    ObjectSerializer seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);
                    SqoTypeInfo ti = await seralizer.DeserializeSqoTypeInfoAsync(false).ConfigureAwait(false);
                    if (ti != null)
                    {
                        list.Add(ti);
                    }
                }
                return list;
            }
            else //elevatedTrust
            { 
#if SL4
             if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }


               

                List<SqoTypeInfo> list = new List<SqoTypeInfo>();

                System.IO.DirectoryInfo di = new System.IO.DirectoryInfo(path);
                foreach (FileInfo f in di.EnumerateFiles("*"+extension))
                {
                    string typeName = f.Name.Replace(extension, "");
                    if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo") || typeName.StartsWith("sqoDB.Indexes.BTreeNode"))//engine types
                {
                    continue;
                }
                    ObjectSerializer seralizer = SerializerFactory.GetSerializer(path, typeName,useElevatedTrust);

                    SqoTypeInfo ti = await seralizer.DeserializeSqoTypeInfoAsync(false).ConfigureAwait(false);
                    if (ti != null)
                    {
                        list.Add(ti);
                    }
                }
                return list;
#else
                return null; // will never be here
#endif

            }
#elif WinRT
            //List<string> fileFilter = new List<string>();
           // fileFilter.Add("*");
            //QueryOptions qo = new QueryOptions();
           // qo.UserSearchFilter = extension;
            //StorageFileQueryResult resultQuery = storageFolder.CreateFileQueryWithOptions(qo);
            IReadOnlyList<IStorageFile> files = await storageFolder.GetFilesAsync();

            List<SqoTypeInfo> list = new List<SqoTypeInfo>();


            foreach (IStorageFile f in files)
            {
                if (f.FileType != extension)
                    continue;
                string typeName = f.Name.Replace(extension, "");
                if (typeName.StartsWith("sqoDB.Indexes.IndexInfo2.") || typeName.StartsWith("sqoDB.MetaObjects.RawdataInfo."))//engine types
                {
                    continue;
                }
                ObjectSerializer seralizer =
 SerializerFactory.GetSerializer(storageFolder.Path, typeName, useElevatedTrust);

                SqoTypeInfo ti = await seralizer.DeserializeSqoTypeInfoAsync(false).ConfigureAwait(false);
                if (ti != null)
                {
                    ti.FileNameForManager = typeName;
                    list.Add(ti);
                }
            }
            return list;
#else
            var list = new List<SqoTypeInfo>();

            var di = new DirectoryInfo(path);
            //TODO: throw exception
            var fi = di.GetFiles("*" + extension);

            foreach (var f in fi)
            {
                var typeName = f.Name.Replace(extension, "");
                if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo") ||
                    typeName.StartsWith("sqoDB.Indexes.BTreeNode")) //engine types
                    continue;
                var seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);

                var ti = await seralizer.DeserializeSqoTypeInfoAsync(false).ConfigureAwait(false);
                if (ti != null && !ti.TypeName.StartsWith("sqoDB.Indexes.BTreeNode"))
                {
                    ti.FileNameForManager = typeName;
                    list.Add(ti);
                }
            }

            return list;
#endif
        }

#endif
        internal void LoadAllTypes()
        {
#if SILVERLIGHT
            string extension = ".sqo";
            if (SiaqodbConfigurator.EncryptedDatabase)
            {
                extension = ".esqo";
            }

            if (!this.useElevatedTrust)
            {
                IsolatedStorageFile isf = IsolatedStorageFile.GetUserStoreForApplication();
                if (isf.DirectoryExists(path))
                {
                    //isf.Remove();
                    //isf = IsolatedStorageFile.GetUserStoreForApplication();

                    //isf.CreateDirectory(path);
                }
                else
                {
                    isf.CreateDirectory(path);
                }
                this.LoadMetaDataTypes();

                string searchPath = Path.Combine(path, "*"+extension);
                string[] files = isf.GetFileNames(searchPath);

                foreach (string f in files)
                {
                    string typeName = f.Replace(extension, "");
                    //System.Reflection.Assembly a = System.Reflection.Assembly.Load(typeName.Split(',')[1]);
                    //Type t = Type.GetType(typeName);
                    if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo"))//engine types
                    {
                        continue;
                    }
                    ObjectSerializer seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);
                    SqoTypeInfo ti = seralizer.DeserializeSqoTypeInfo(true);
                   
                    if (ti != null)
                    {
                        if (this.GetFileByType(ti) != typeName)//check for custom fileName
                        {
                            continue;
                        }

                        this.CompareSchema(ti);
                    }
                   


                }
            }
            else //elevatedTrust
            {
#if SL4
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }
                this.LoadMetaDataTypes();

                System.IO.DirectoryInfo di = new System.IO.DirectoryInfo(path);
                foreach (FileInfo f in di.EnumerateFiles("*"+extension))
                {
                    string typeName = f.Name.Replace(extension, "");
                    if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo"))//engine types
                    {
                        continue;
                    }
                    ObjectSerializer seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);

                    SqoTypeInfo ti = seralizer.DeserializeSqoTypeInfo(true);

               
                    if (ti != null)
                    {
                        if (this.GetFileByType(ti) != typeName)//check for custom fileName
                        {
                            continue;
                        }

                        this.CompareSchema(ti);
                    }
             


                }
#endif

            }
#elif WinRT
            this.LoadMetaDataTypes();

            string extension = ".sqo";
            if (SiaqodbConfigurator.EncryptedDatabase)
            {
                extension = ".esqo";
            }
            //List<string> fileFilter = new List<string>();
            ////fileFilter.Add("*");
            //QueryOptions qo = new QueryOptions();
            //qo.UserSearchFilter = extension;
            //StorageFileQueryResult resultQuery = storageFolder.CreateFileQueryWithOptions(qo);
            IReadOnlyList<StorageFile> files = storageFolder.GetFilesAsync().AsTask().Result;

            List<SqoTypeInfo> listToBuildIndexes = new List<SqoTypeInfo>();
            foreach (StorageFile f in files)
            {
                if (f.FileType != extension)
                    continue;

                string typeName = f.Name.Replace(extension, "");

                //Type t=Type.GetType(typeName);
                if (typeName.StartsWith("indexinfo2.") || typeName.StartsWith("rawdatainfo."))//engine types
                {
                    continue;
                }
                ObjectSerializer seralizer =
 SerializerFactory.GetSerializer(storageFolder.Path, typeName, this.useElevatedTrust);

                SqoTypeInfo ti = seralizer.DeserializeSqoTypeInfo(true);




                if (ti != null)
                {
                    if (this.GetFileByType(ti) != typeName)//check for custom fileName
                    {
                        continue;
                    }

                    this.CompareSchema(ti);

                }


            }
#else

            if (Directory.Exists(path))
            {
                LoadMetaDataTypes();
                var di = new DirectoryInfo(path);

                //TODO: throw exception
                var extension = ".sqo";
                if (SiaqodbConfigurator.EncryptedDatabase) extension = ".esqo";
                var fi = di.GetFiles("*" + extension);

                var listToBuildIndexes = new List<SqoTypeInfo>();
                foreach (var f in fi)
                {
                    var typeName = f.Name.Replace(extension, "");

                    //Type t=Type.GetType(typeName);
                    if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo")) //engine types
                        continue;
                    var seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);

                    var ti = seralizer.DeserializeSqoTypeInfo(true);


                    if (ti != null)
                    {
                        if (GetFileByType(ti) != typeName) //check for custom fileName
                            continue;

                        CompareSchema(ti);
                    }
                }
            }
            else
            {
                throw new SiaqodbException("Invalid folder path!");
            }
#endif
        }

#if ASYNC
        internal async Task LoadAllTypesAsync()
        {
#if SILVERLIGHT
            string extension = ".sqo";
            if (SiaqodbConfigurator.EncryptedDatabase)
            {
                extension = ".esqo";
            }

            if (!this.useElevatedTrust)
            {
                IsolatedStorageFile isf = IsolatedStorageFile.GetUserStoreForApplication();
                if (isf.DirectoryExists(path))
                {
                    //isf.Remove();
                    //isf = IsolatedStorageFile.GetUserStoreForApplication();

                    //isf.CreateDirectory(path);
                }
                else
                {
                    isf.CreateDirectory(path);
                }
                await this.LoadMetaDataTypesAsync().ConfigureAwait(false);

                string searchPath = Path.Combine(path, "*"+extension);
                string[] files = isf.GetFileNames(searchPath);

                foreach (string f in files)
                {
                    string typeName = f.Replace(extension, "");
                    //System.Reflection.Assembly a = System.Reflection.Assembly.Load(typeName.Split(',')[1]);
                    //Type t = Type.GetType(typeName);
                    if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo"))//engine types
                    {
                        continue;
                    }
                    ObjectSerializer seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);
                    SqoTypeInfo ti = await seralizer.DeserializeSqoTypeInfoAsync(true).ConfigureAwait(false);
                   
                    if (ti != null)
                    {
                        if (this.GetFileByType(ti) != typeName)//check for custom fileName
                        {
                            continue;
                        }

                        await this.CompareSchemaAsync(ti).ConfigureAwait(false);
                    }
                   


                }
            }
            else //elevatedTrust
            {
#if SL4
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }
                await this.LoadMetaDataTypesAsync().ConfigureAwait(false);

                System.IO.DirectoryInfo di = new System.IO.DirectoryInfo(path);
                foreach (FileInfo f in di.EnumerateFiles("*"+extension))
                {
                    string typeName = f.Name.Replace(extension, "");
                    if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo"))//engine types
                    {
                        continue;
                    }
                    ObjectSerializer seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);

                    SqoTypeInfo ti = await seralizer.DeserializeSqoTypeInfoAsync(true).ConfigureAwait(false);

               
                    if (ti != null)
                    {
                        if (this.GetFileByType(ti) != typeName)//check for custom fileName
                        {
                            continue;
                        }

                       await this.CompareSchemaAsync(ti).ConfigureAwait(false);
                    }
             


                }
#endif

            }
#elif WinRT
            await this.LoadMetaDataTypesAsync().ConfigureAwait(false);
            string extension = ".sqo";
            if (SiaqodbConfigurator.EncryptedDatabase)
            {
                extension = ".esqo";
            }
           
           // List<string> fileFilter = new List<string>();
            //fileFilter.Add("*");
            //QueryOptions qo = new QueryOptions();
            //qo.UserSearchFilter = extension;
            //StorageFileQueryResult resultQuery = storageFolder.CreateFileQueryWithOptions(qo);
            IReadOnlyList<StorageFile> files = await storageFolder.GetFilesAsync();

            List<SqoTypeInfo> listToBuildIndexes = new List<SqoTypeInfo>();
            foreach (StorageFile f in files)
            {
                if (f.FileType != extension)
                {
                    continue;
                }

                string typeName = f.Name.Replace(extension, "");

                //Type t=Type.GetType(typeName);
                if (typeName.StartsWith("indexinfo2.") || typeName.StartsWith("rawdatainfo."))//engine types
                {
                    continue;
                }
                ObjectSerializer seralizer =
 SerializerFactory.GetSerializer(storageFolder.Path, typeName, this.useElevatedTrust);

                SqoTypeInfo ti = await seralizer.DeserializeSqoTypeInfoAsync(true).ConfigureAwait(false);




                if (ti != null)
                {
                    if (this.GetFileByType(ti) != typeName)//check for custom fileName
                    {
                        continue;
                    }

                    await this.CompareSchemaAsync(ti).ConfigureAwait(false);

                }


            }

#else

            if (Directory.Exists(path))
            {
                await LoadMetaDataTypesAsync().ConfigureAwait(false);
                var di = new DirectoryInfo(path);

                //TODO: throw exception
                var extension = ".sqo";
                if (SiaqodbConfigurator.EncryptedDatabase) extension = ".esqo";
                var fi = di.GetFiles("*" + extension);

                var listToBuildIndexes = new List<SqoTypeInfo>();
                foreach (var f in fi)
                {
                    var typeName = f.Name.Replace(extension, "");

                    //Type t=Type.GetType(typeName);
                    if (typeName.StartsWith("indexinfo") || typeName.StartsWith("rawdatainfo")) //engine types
                        continue;
                    var seralizer = SerializerFactory.GetSerializer(path, typeName, useElevatedTrust);
                    var ti = await seralizer.DeserializeSqoTypeInfoAsync(true).ConfigureAwait(false);
                    if (ti != null)
                    {
                        if (GetFileByType(ti) != typeName) //check for custom fileName
                            continue;
                        await CompareSchemaAsync(ti).ConfigureAwait(false);
                    }
                }
            }
            else
            {
                throw new SiaqodbException("Invalid folder path!");
            }
#endif
        }

#endif
        private void CompareSchema(SqoTypeInfo ti)
        {
            var actualType = MetaExtractor.GetSqoTypeInfo(ti.Type);
            if (!MetaExtractor.CompareSqoTypeInfos(actualType, ti))
            {
                var table = LoadAll(ti);

                try
                {
                    actualType.Header.numberOfRecords = ti.Header.numberOfRecords;
                    actualType.Header.TID = ti.Header.TID;

                    SaveType(actualType);
                    var serializer = SerializerFactory.GetSerializer(path, GetFileByType(actualType), useElevatedTrust);
                    serializer.SaveObjectTable(actualType, ti, table, rawSerializer);

                    if (GetFileByType(actualType) != GetFileByType(ti)) //< version 3.1 on SL
                        DropType(ti);
                    metaCache.AddType(actualType.Type, actualType);

                    Flush(actualType);
                }
                catch
                {
                    SiaqodbConfigurator.LogMessage("Type:" + ti.Type + " cannot be upgraded, will be marked as 'Old'!",
                        VerboseLevel.Error);
                    ti.IsOld = true;
                    SaveType(ti);
                    metaCache.AddType(ti.Type, ti);
                }
            }
            else
            {
#if SILVERLIGHT
                //hack
                if (ti.Type==typeof(Indexes.IndexInfo2) && ti.Header.version > -35 )
                {
                    this.SaveType(actualType);
                    metaCache.AddType(actualType.Type, actualType);
                    return;
                }
#endif
                metaCache.AddType(ti.Type, ti);
            }
        }
#if ASYNC
        private async Task CompareSchemaAsync(SqoTypeInfo ti)
        {
            var actualType = MetaExtractor.GetSqoTypeInfo(ti.Type);
            if (!MetaExtractor.CompareSqoTypeInfos(actualType, ti))
            {
                var table = await LoadAllAsync(ti).ConfigureAwait(false);
                var typeWasSaved = true;
                try
                {
                    actualType.Header.numberOfRecords = ti.Header.numberOfRecords;
                    actualType.Header.TID = ti.Header.TID;

                    await SaveTypeAsync(actualType).ConfigureAwait(false);
                    var serializer = SerializerFactory.GetSerializer(path, GetFileByType(actualType), useElevatedTrust);
                    await serializer.SaveObjectTableAsync(actualType, ti, table, rawSerializer).ConfigureAwait(false);

                    if (GetFileByType(actualType) != GetFileByType(ti)) //< version 3.1 on SL
                        await DropTypeAsync(ti).ConfigureAwait(false);
                    metaCache.AddType(actualType.Type, actualType);

                    await FlushAsync(actualType).ConfigureAwait(false);
                }
                catch
                {
                    SiaqodbConfigurator.LogMessage("Type:" + ti.Type + " cannot be upgraded, will be marked as 'Old'!",
                        VerboseLevel.Error);

                    ti.IsOld = true;
                    typeWasSaved = false;
                }

                if (!typeWasSaved)
                {
                    await SaveTypeAsync(ti).ConfigureAwait(false);
                    metaCache.AddType(ti.Type, ti);
                }
            }
            else
            {
#if SILVERLIGHT
                //hack
                if (ti.Type==typeof(Indexes.IndexInfo2) && ti.Header.version > -35 )
                {
                    await this.SaveTypeAsync(actualType).ConfigureAwait(false);
                    metaCache.AddType(actualType.Type, actualType);
                    return;
                }
#endif
                metaCache.AddType(ti.Type, ti);
            }
        }
#endif
        internal bool DropType(SqoTypeInfo ti)
        {
            return DropType(ti, false);
        }
#if ASYNC
        internal async Task<bool> DropTypeAsync(SqoTypeInfo ti)
        {
            return await DropTypeAsync(ti, false).ConfigureAwait(false);
        }
#endif
        internal bool DropType(SqoTypeInfo ti, bool claimFreeSpace)
        {
            lock (_syncRoot)
            {
                if (claimFreeSpace) MarkFreeSpace(ti);
                var fileName = "";
                if (SiaqodbConfigurator.EncryptedDatabase)
                    fileName = path + Path.DirectorySeparatorChar + GetFileByType(ti) + ".esqo";
                else
                    fileName = path + Path.DirectorySeparatorChar + GetFileByType(ti) + ".sqo";
#if SILVERLIGHT
                if (!this.useElevatedTrust)
                {
                    IsolatedStorageFile isf = IsolatedStorageFile.GetUserStoreForApplication();

                    if (isf.FileExists(fileName))
                    {
                        ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.path, GetFileByType(ti), useElevatedTrust);
                        serializer.Close();

                        try
                        {
                            isf.DeleteFile(fileName);

                            return true;
                        }
                        catch (IsolatedStorageException ex)
                        {
                            throw ex;
                        }
                    }
                }
                else
                {
                    if (File.Exists(fileName))
                    {
                        ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.path, GetFileByType(ti), this.useElevatedTrust);
                        serializer.Close();
                        File.Delete(fileName);
                        return true;
                    }   
                }


#elif MONODROID
				if (File.Exists(fileName))
				{
					ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.path, GetFileByType(ti), this.useElevatedTrust);
					serializer.Close();
					try
					{
						File.Delete(fileName);
						return true;
					}
					catch (UnauthorizedAccessException ex) //monodroid bug!!!:https://bugzilla.novell.com/show_bug.cgi?id=684172
					{
						SiaqodbConfigurator.LogMessage("File:"+fileName+" cannot be deleted,set size to zero!",VerboseLevel.Error);

						serializer.Open(this.useElevatedTrust);
						serializer.MakeEmpty();
						serializer.Close();
						return true;
					}
				}

#elif UNITY3D
                if (File.Exists(fileName))
                {
                    ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.path, GetFileByType(ti), this.useElevatedTrust);
                    serializer.Close();
                    try
                    {
                        File.Delete(fileName);
                        return true;
                    }
                    catch (UnauthorizedAccessException ex) //monodroid bug!!!:https://bugzilla.novell.com/show_bug.cgi?id=684172
                    {
                        SiaqodbConfigurator.LogMessage("File:"+fileName+" cannot be deleted,set size to zero!",VerboseLevel.Error);
                  
                        serializer.Open(this.useElevatedTrust);
                        serializer.MakeEmpty();
                        serializer.Close();
                        return true;
                    }
                }
#elif WinRT
                ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.storageFolder.Path, GetFileByType(ti), useElevatedTrust);
                serializer.Close();
                StorageFolder storageFolder =
 StorageFolder.GetFolderFromPathAsync(this.storageFolder.Path).AsTask().Result;
                try
                {
                    StorageFile file = storageFolder.GetFileAsync(Path.GetFileName(fileName)).AsTask().Result;

                    file.DeleteAsync(StorageDeleteOption.PermanentDelete).AsTask().Wait();
                }
                catch (FileNotFoundException ex)
                {
                    return false;
                }
                return true;


#else
                if (File.Exists(fileName))
                {
                    var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
                    serializer.Close();
                    File.Delete(fileName);
                    return true;
                }


#endif
                return false;
            }
        }

#if ASYNC
        internal async Task<bool> DropTypeAsync(SqoTypeInfo ti, bool claimFreeSpace)
        {
            if (claimFreeSpace) await MarkFreeSpaceAsync(ti).ConfigureAwait(false);
            var fileName = "";
            if (SiaqodbConfigurator.EncryptedDatabase)
                fileName = path + Path.DirectorySeparatorChar + GetFileByType(ti) + ".esqo";
            else
                fileName = path + Path.DirectorySeparatorChar + GetFileByType(ti) + ".sqo";
#if SILVERLIGHT
                if (!this.useElevatedTrust)
                {
                    IsolatedStorageFile isf = IsolatedStorageFile.GetUserStoreForApplication();

                    if (isf.FileExists(fileName))
                    {
                        ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.path, GetFileByType(ti), useElevatedTrust);
                        await serializer.CloseAsync().ConfigureAwait(false);

                        try
                        {
                            isf.DeleteFile(fileName);

                            return true;
                        }
                        catch (IsolatedStorageException ex)
                        {
                            throw ex;
                        }
                    }
                }
                else
                {
                    if (File.Exists(fileName))
                    {
                        ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.path, GetFileByType(ti), this.useElevatedTrust);
                        await serializer.CloseAsync().ConfigureAwait(false);
                        File.Delete(fileName);
                        return true;
                    }   
                }


#elif MONODROID
			if (File.Exists(fileName))
			{
				ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.path, GetFileByType(ti), this.useElevatedTrust);
				serializer.Close();
				try
				{
					File.Delete(fileName);
					return true;
				}
				catch (UnauthorizedAccessException ex) //monodroid bug!!!:https://bugzilla.novell.com/show_bug.cgi?id=684172
				{
					SiaqodbConfigurator.LogMessage("File:"+fileName+" cannot be deleted,set size to zero!",VerboseLevel.Error);

					serializer.Open(this.useElevatedTrust);
					serializer.MakeEmpty();
					serializer.Close();
					return true;
				}
			}

#elif UNITY3D
                if (File.Exists(fileName))
                {
                    ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.path, GetFileByType(ti), this.useElevatedTrust);
                    serializer.Close();
                    try
                    {
                        File.Delete(fileName);
                        return true;
                    }
                    catch (UnauthorizedAccessException ex) //monodroid bug!!!:https://bugzilla.novell.com/show_bug.cgi?id=684172
                    {
                        SiaqodbConfigurator.LogMessage("File:"+fileName+" cannot be deleted,set size to zero!",VerboseLevel.Error);
                  
                        serializer.Open(this.useElevatedTrust);
                        serializer.MakeEmpty();
                        serializer.Close();
                        return true;
                    }
                }
#elif WinRT
                ObjectSerializer serializer =
 SerializerFactory.GetSerializer(this.storageFolder.Path, GetFileByType(ti), useElevatedTrust);
                serializer.Close();
                StorageFolder storageFolder = await StorageFolder.GetFolderFromPathAsync(this.storageFolder.Path);
                try
                {
                    StorageFile file = await storageFolder.GetFileAsync(Path.GetFileName(fileName));

                    await file.DeleteAsync(StorageDeleteOption.PermanentDelete);
                }
                catch (FileNotFoundException ex)
                {
                    return false;
                }
                return true;


#else
            if (File.Exists(fileName))
            {
                var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
                await serializer.CloseAsync().ConfigureAwait(false);
                File.Delete(fileName);
                return true;
            }


#endif
            return false;
        }

#endif
        internal SqoTypeInfo GetSqoTypeInfo(string typeName)
        {
            var seralizer = SerializerFactory.GetSerializer(path, GetFileByType(typeName), useElevatedTrust);

            var ti = seralizer.DeserializeSqoTypeInfo(false);
            return ti;
        }
#if ASYNC
        internal async Task<SqoTypeInfo> GetSqoTypeInfoAsync(string typeName)
        {
            var seralizer = SerializerFactory.GetSerializer(path, GetFileByType(typeName), useElevatedTrust);

            var ti = await seralizer.DeserializeSqoTypeInfoAsync(false).ConfigureAwait(false);
            return ti;
        }
#endif
        private void UpgradeInternalSqoTypeInfos(Type type, string newName, bool dropIt)
        {
            var seralizer =
                SerializerFactory.GetSerializer(path, MetaHelper.GetOldFileNameByType(type), useElevatedTrust);
            var ti = seralizer.DeserializeSqoTypeInfo(true);
            if (ti != null)
            {
                if (dropIt)
                {
                    DropType(ti);
                    var actualType1 = MetaExtractor.GetSqoTypeInfo(ti.Type);

                    CacheCustomFileNames.AddFileNameForType(actualType1.TypeName, newName, false);
                    return;
                }

                var table = LoadAll(ti);
                DropType(ti);
                var actualType = MetaExtractor.GetSqoTypeInfo(ti.Type);
                CacheCustomFileNames.AddFileNameForType(actualType.TypeName, newName, false);

                try
                {
                    actualType.Header.numberOfRecords = ti.Header.numberOfRecords;
                    actualType.Header.TID = ti.Header.TID;

                    SaveType(actualType);
                    var serializer = SerializerFactory.GetSerializer(path, GetFileByType(actualType), useElevatedTrust);
                    serializer.SaveObjectTable(actualType, ti, table, rawSerializer);

                    metaCache.AddType(actualType.Type, actualType);

                    Flush(actualType);
                }
                catch
                {
                    SiaqodbConfigurator.LogMessage("Type:" + ti.Type + " cannot be upgraded, will be marked as 'Old'!",
                        VerboseLevel.Error);

                    ti.IsOld = true;
                    SaveType(ti);
                    metaCache.AddType(ti.Type, ti);
                }
            }
        }
#if ASYNC
        private async Task UpgradeInternalSqoTypeInfosAsync(Type type, string newName, bool dropIt)
        {
            var seralizer =
                SerializerFactory.GetSerializer(path, MetaHelper.GetOldFileNameByType(type), useElevatedTrust);
            var ti = await seralizer.DeserializeSqoTypeInfoAsync(true).ConfigureAwait(false);
            if (ti != null)
            {
                if (dropIt)
                {
                    await DropTypeAsync(ti).ConfigureAwait(false);
                    var actualType1 = MetaExtractor.GetSqoTypeInfo(ti.Type);

                    CacheCustomFileNames.AddFileNameForType(actualType1.TypeName, newName, false);
                    return;
                }

                var table = await LoadAllAsync(ti).ConfigureAwait(false);
                await DropTypeAsync(ti).ConfigureAwait(false);
                var actualType = MetaExtractor.GetSqoTypeInfo(ti.Type);
                CacheCustomFileNames.AddFileNameForType(actualType.TypeName, newName, false);
                var exThr = false;
                try
                {
                    actualType.Header.numberOfRecords = ti.Header.numberOfRecords;
                    actualType.Header.TID = ti.Header.TID;

                    await SaveTypeAsync(actualType).ConfigureAwait(false);
                    var serializer = SerializerFactory.GetSerializer(path, GetFileByType(actualType), useElevatedTrust);
                    await serializer.SaveObjectTableAsync(actualType, ti, table, rawSerializer).ConfigureAwait(false);

                    metaCache.AddType(actualType.Type, actualType);

                    await FlushAsync(actualType).ConfigureAwait(false);
                }
                catch
                {
                    SiaqodbConfigurator.LogMessage("Type:" + ti.Type + " cannot be upgraded, will be marked as 'Old'!",
                        VerboseLevel.Error);

                    ti.IsOld = true;
                    exThr = true;
                }

                if (exThr)
                {
                    await SaveTypeAsync(ti).ConfigureAwait(false);
                    metaCache.AddType(ti.Type, ti);
                }
            }
        }
#endif

        #endregion


        #region TRANSACTIONS

        public void RecoverAfterCrash(SqoTypeInfo tiObjectHeader, SqoTypeInfo tiType)
        {
            lock (_syncRoot)
            {
                IList<TransactionObjectHeader> headers = LoadAll<TransactionObjectHeader>(tiObjectHeader);

                if (headers.Count > 0)
                {
                    var storage = GetTransactionLogStorage();

                    foreach (var header in headers)
                    {
                        var objectBytes = new byte[header.BatchSize];
                        storage.Read(header.Position, objectBytes);
                        var objType = Type.GetType(header.TypeName);
                        SqoTypeInfo tiObject = null;

                        if (metaCache.Contains(objType))
                            tiObject = metaCache.GetSqoTypeInfo(objType);
                        else
                            tiObject = MetaExtractor.GetSqoTypeInfo(objType);

                        if (tiObject.Header.lengthOfRecord != header.BatchSize)
                            throw new SiaqodbException(
                                "Type schema is different,so objects cannot be rollback after crash");

                        var serializer =
                            SerializerFactory.GetSerializer(path, GetFileByType(tiObject), useElevatedTrust);
                        serializer.SerializeObject(objectBytes, header.OIDofObject, tiObject, false);
                    }

                    storage.Close();
                }

                IList<TransactionTypeHeader> tHeaders = LoadAll<TransactionTypeHeader>(tiType);
                foreach (var tHeader in tHeaders)
                {
                    //reset number of records   
                    var t = Type.GetType(tHeader.TypeName);
                    SqoTypeInfo ti = null;

                    if (metaCache.Contains(t))
                        ti = metaCache.GetSqoTypeInfo(t);
                    else
                        ti = MetaExtractor.GetSqoTypeInfo(t);
                    var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
                    serializer.SaveNrRecords(ti, tHeader.NumberOfRecords);

                    indexManager.ReBuildIndexesAfterCrash(ti);
                }

                DropType(tiObjectHeader);
                DropType(tiType);
                DropTransactionLog();
            }
        }
#if ASYNC
        public async Task RecoverAfterCrashAsync(SqoTypeInfo tiObjectHeader, SqoTypeInfo tiType)
        {
            IList<TransactionObjectHeader> headers =
                await LoadAllAsync<TransactionObjectHeader>(tiObjectHeader).ConfigureAwait(false);

            if (headers.Count > 0)
            {
                var storage = GetTransactionLogStorage();

                foreach (var header in headers)
                {
                    var objectBytes = new byte[header.BatchSize];
                    await storage.ReadAsync(header.Position, objectBytes).ConfigureAwait(false);
                    var objType = Type.GetType(header.TypeName);
                    SqoTypeInfo tiObject = null;

                    if (metaCache.Contains(objType))
                        tiObject = metaCache.GetSqoTypeInfo(objType);
                    else
                        tiObject = MetaExtractor.GetSqoTypeInfo(objType);

                    if (tiObject.Header.lengthOfRecord != header.BatchSize)
                        throw new SiaqodbException(
                            "Type schema is different,so objects cannot be rollback after crash");

                    var serializer = SerializerFactory.GetSerializer(path, GetFileByType(tiObject), useElevatedTrust);
                    await serializer.SerializeObjectAsync(objectBytes, header.OIDofObject, tiObject, false)
                        .ConfigureAwait(false);
                }

                await storage.CloseAsync().ConfigureAwait(false);
            }

            IList<TransactionTypeHeader> tHeaders =
                await LoadAllAsync<TransactionTypeHeader>(tiType).ConfigureAwait(false);
            foreach (var tHeader in tHeaders)
            {
                //reset number of records   
                var t = Type.GetType(tHeader.TypeName);
                SqoTypeInfo ti = null;

                if (metaCache.Contains(t))
                    ti = metaCache.GetSqoTypeInfo(t);
                else
                    ti = MetaExtractor.GetSqoTypeInfo(t);
                var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
                await serializer.SaveNrRecordsAsync(ti, tHeader.NumberOfRecords).ConfigureAwait(false);

                await indexManager.ReBuildIndexesAfterCrashAsync(ti).ConfigureAwait(false);
            }

            await DropTypeAsync(tiObjectHeader).ConfigureAwait(false);
            await DropTypeAsync(tiType).ConfigureAwait(false);
            DropTransactionLog();
        }
#endif
        public bool DropTransactionLog()
        {
            lock (_syncRoot)
            {
                var fileName = path + Path.DirectorySeparatorChar + "transactionlog.slog";

#if SILVERLIGHT
                if (!this.useElevatedTrust)
                {
                    IsolatedStorageFile isf = IsolatedStorageFile.GetUserStoreForApplication();

                    if (isf.FileExists(fileName))
                    {
                        try
                        {
                            isf.DeleteFile(fileName);

                            return true;
                        }
                        catch (IsolatedStorageException ex)
                        {
                            throw ex;
                        }
                    }
                }
                else
                {
                    if (File.Exists(fileName))
                    {
                        File.Delete(fileName);
                        return true;
                    }
                }


#elif MONODROID
				if (File.Exists(fileName))
				{
					try
					{
						File.Delete(fileName);
						return true;
					}
					catch (UnauthorizedAccessException ex) //monodroid bug!!!:https://bugzilla.novell.com/show_bug.cgi?id=684172
					{
						FileStream file = new FileStream(fileName, FileMode.OpenOrCreate,FileAccess.ReadWrite);
						file.SetLength(0);
						file.Close();
						return true;
					}
				}
#elif UNITY3D
                if (File.Exists(fileName))
                {
                    try
                    {
                        File.Delete(fileName);
                        return true;
                    }
                    catch (UnauthorizedAccessException ex) //monodroid bug!!!:https://bugzilla.novell.com/show_bug.cgi?id=684172
                    {
                        FileStream file = new FileStream(fileName, FileMode.OpenOrCreate,FileAccess.ReadWrite);
                        file.SetLength(0);
                        file.Close();
                        return true;
                    }
                }
#elif WinRT
                try
                {
                    StorageFolder storageFolder =
 StorageFolder.GetFolderFromPathAsync(this.storageFolder.Path).AsTask().Result;

                    StorageFile file = storageFolder.GetFileAsync(Path.GetFileName(fileName)).AsTask().Result;
                    file.DeleteAsync(StorageDeleteOption.PermanentDelete).AsTask().Wait();
                }
                catch (AggregateException ae)
                {
                    foreach (var e in ae.InnerExceptions)
                    {
                        if (e is FileNotFoundException)
                        {
                            return false;
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
                catch (FileNotFoundException ex)
                {
                    return false;
                }
            return true;
#else
                if (File.Exists(fileName))
                {
                    File.Delete(fileName);
                    return true;
                }
#endif
                return false;
            }
        }

        public TransactionsStorage GetTransactionLogStorage()
        {
            var fileFull = path + Path.DirectorySeparatorChar + "transactionlog.slog";
            var transactStorage = new TransactionsStorage(fileFull, useElevatedTrust);
            return transactStorage;
        }

        internal void RollbackObject(object oi, SqoTypeInfo ti)
        {
            var objInfo = MetaExtractor.GetObjectInfo(oi, ti, metaCache);

            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedSaveComplexObject += serializer_NeedSaveComplexObject;

            var oldValuesOfIndexedFields = indexManager.PrepareUpdateIndexes(objInfo, ti);

            serializer.SerializeObject(objInfo, rawSerializer);

            indexManager.UpdateIndexes(objInfo, ti, oldValuesOfIndexedFields);
        }
#if ASYNC
        internal async Task RollbackObjectAsync(object oi, SqoTypeInfo ti)
        {
            var objInfo = MetaExtractor.GetObjectInfo(oi, ti, metaCache);

            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.NeedSaveComplexObjectAsync += serializer_NeedSaveComplexObjectAsync;

            var oldValuesOfIndexedFields =
                await indexManager.PrepareUpdateIndexesAsync(objInfo, ti).ConfigureAwait(false);

            await serializer.SerializeObjectAsync(objInfo, rawSerializer).ConfigureAwait(false);

            await indexManager.UpdateIndexesAsync(objInfo, ti, oldValuesOfIndexedFields).ConfigureAwait(false);
        }
#endif
        internal void RollbackDeletedObject(object obj, SqoTypeInfo ti)
        {
            var objInfo = MetaExtractor.GetObjectInfo(obj, ti, metaCache);

            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);

            lock (_syncRoot)
            {
                serializer.RollbackDeleteObject(objInfo.Oid, ti);
                indexManager.UpdateIndexes(objInfo, ti, new Dictionary<string, object>());
            }
        }
#if ASYNC
        internal async Task RollbackDeletedObjectAsync(object obj, SqoTypeInfo ti)
        {
            var objInfo = MetaExtractor.GetObjectInfo(obj, ti, metaCache);

            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);


            await serializer.RollbackDeleteObjectAsync(objInfo.Oid, ti).ConfigureAwait(false);
            await indexManager.UpdateIndexesAsync(objInfo, ti, new Dictionary<string, object>()).ConfigureAwait(false);
        }

#endif

        internal void TransactionCommitStatus(bool started)
        {
            rawSerializer.TransactionCommitStatus(started);
        }

        internal byte[] GetObjectBytes(int oid, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return serializer.ReadObjectBytes(oid, ti);
        }
#if ASYNC
        internal async Task<byte[]> GetObjectBytesAsync(int oid, SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            return await serializer.ReadObjectBytesAsync(oid, ti).ConfigureAwait(false);
        }
#endif

        #endregion

        #region EVENTS

#if UNITY3D
        protected virtual void OnLoadingObject(LoadingObjectEventArgs args)
        {
            if (loadingObject != null)
            {
                if (args.ObjectType != typeof(sqoDB.MetaObjects.RawdataInfo) && args.ObjectType != typeof(sqoDB.Indexes.IndexInfo2))
                {
                    loadingObject(this, args);
                }
            }
        }
        protected virtual void OnLoadedObject(int oid,object obj)
        {
            if (loadedObject != null)
            {
                if (obj.GetType() != typeof(sqoDB.MetaObjects.RawdataInfo) && obj.GetType() != typeof(sqoDB.Indexes.IndexInfo2))
                {
                    LoadedObjectEventArgs args = new LoadedObjectEventArgs(oid, obj);
                    loadedObject(this, args);
                }
            }
        }
        protected void OnNeedSaveComplexObject(ComplexObjectEventArgs args)
        {
            if (needSaveComplexObject != null)
            {
                needSaveComplexObject(this, args);
            }
        }
		#if ASYNC
		protected async Task OnNeedSaveComplexObjectAsync(ComplexObjectEventArgs args)
		{
			if (needSaveComplexObjectAsync != null)
			{
				await needSaveComplexObjectAsync(this, args).ConfigureAwait(false);
			}
		}
		#endif
#else
        protected virtual void OnLoadingObject(LoadingObjectEventArgs args)
        {
            if (LoadingObject != null)
                if (args.ObjectType != typeof(RawdataInfo) && args.ObjectType != typeof(IndexInfo2))
                    LoadingObject(this, args);
        }

        protected virtual void OnLoadedObject(int oid, object obj)
        {
            if (LoadedObject != null)
                if (obj.GetType() != typeof(RawdataInfo) && obj.GetType() != typeof(IndexInfo2))
                {
                    var args = new LoadedObjectEventArgs(oid, obj);
                    LoadedObject(this, args);
                }
        }

        protected void OnNeedSaveComplexObject(ComplexObjectEventArgs args)
        {
            if (NeedSaveComplexObject != null) NeedSaveComplexObject(this, args);
        }
#if ASYNC
        protected async Task OnNeedSaveComplexObjectAsync(ComplexObjectEventArgs args)
        {
            if (NeedSaveComplexObjectAsync != null) await NeedSaveComplexObjectAsync(this, args).ConfigureAwait(false);
        }
#endif


#endif
#if UNITY3D || CF || MONODROID
#else
        protected void OnIndexesSaveAsyncFinished(IndexesSaveAsyncFinishedArgs e)
        {
            if (IndexesSaveAsyncFinished != null) IndexesSaveAsyncFinished(this, e);
        }
#endif

        #endregion

        #region OPERATIONS

        internal void Close()
        {
            lock (_syncRoot)
            {
                SerializerFactory.CloseAll();
                rawSerializer.Close();
            }
        }
#if ASYNC
        internal async Task CloseAsync()
        {
            await SerializerFactory.CloseAllAsync().ConfigureAwait(false);
            await rawSerializer.CloseAsync().ConfigureAwait(false);
        }
#endif
        internal void Flush()
        {
            SerializerFactory.FlushAll();
            rawSerializer.Flush();
        }
#if ASYNC
        internal async Task FlushAsync()
        {
            await SerializerFactory.FlushAllAsync().ConfigureAwait(false);
            await rawSerializer.FlushAsync().ConfigureAwait(false);
        }
#endif
        internal void Flush(SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            serializer.Flush();
        }
#if ASYNC
        internal async Task FlushAsync(SqoTypeInfo ti)
        {
            var serializer = SerializerFactory.GetSerializer(path, GetFileByType(ti), useElevatedTrust);
            await serializer.FlushAsync().ConfigureAwait(false);
        }
#endif


        internal ISqoFile GetRawFile()
        {
            return rawSerializer.File;
        }

        internal string GetFileName(SqoTypeInfo ti)
        {
            var fileName = "";
            if (SiaqodbConfigurator.EncryptedDatabase)
                fileName = path + Path.DirectorySeparatorChar + GetFileByType(ti) + ".esqo";
            else
                fileName = path + Path.DirectorySeparatorChar + GetFileByType(ti) + ".sqo";
            return fileName;
        }
    }

    #endregion
}