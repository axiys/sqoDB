#if WinRT
using Windows.Storage;
#endif
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Xml;
using sqoDB.Cache;
using sqoDB.Core;
using sqoDB.Exceptions;
using sqoDB.Indexes;
using sqoDB.Meta;
using sqoDB.MetaObjects;
using sqoDB.Queries;
using sqoDB.Transactions;
using sqoDB.Utilities;
#if ASYNC
using System.Threading.Tasks;
#endif


namespace sqoDB
{
    /// <summary>
    ///     Main class of siaqodb database engine responsible for storing, retrieving ,deleting objects on database files
    /// </summary>
    [Obfuscation(Feature = "Apply to member * when event: all", Exclude = false, ApplyToMembers = true)]
#if KEVAST
    internal
#else
    public
#endif
        class Siaqodb : ISiaqodb
    {
        private readonly object _syncRoot = new object();
#if ASYNC
        private readonly AsyncLock _lockerAsync = new AsyncLock();
#endif
        private readonly object _locker = new object();


#if WinRT
        StorageFolder databaseFolder;
#endif
        private string path;
        private StorageEngine storageEngine;
        private CacheForManager cacheForManager;
        internal MetaCache metaCache;
        private IndexManager indexManager;
        private bool opened;
        internal List<object> circularRefCache = new List<object>();

        private bool
            storeOnlyReferencesOfListItems; //used only in StoreObjectPartially to store only references of list items

        /// <summary>
        ///     Raised before an object is saved in database
        /// </summary>
#if UNITY3D
        private EventHandler<SavingEventsArgs> savingObject;
        public event EventHandler<SavingEventsArgs> SavingObject
        {
            add
            {
                lock (_syncRoot)
                {
                    savingObject += value;
                }
            }
            remove
            {
                lock (_syncRoot)
                {
                    savingObject -= value;
                }
            }

        }
#else
        public event EventHandler<SavingEventsArgs> SavingObject;
#endif

        /// <summary>
        ///     Raised after an object is saved in database
        /// </summary>
#if UNITY3D
        private EventHandler<SavedEventsArgs> savedObject;
        public event EventHandler<SavedEventsArgs> SavedObject
        {
            add
            {
                lock (_syncRoot)
                {
                    savedObject += value;
                }
            }
            remove
            {
                lock (_syncRoot)
                {
                    savedObject -= value;
                }
            }

        }
#else
        public event EventHandler<SavedEventsArgs> SavedObject;
#endif


        /// <summary>
        ///     Raised before an object is deleted from database
        /// </summary>
#if UNITY3D
        private EventHandler<DeletingEventsArgs> deletingObject;
        public event EventHandler<DeletingEventsArgs> DeletingObject
        {
            add
            {
                lock (_syncRoot)
                {
                    deletingObject += value;
                }
            }
            remove
            {
                lock (_syncRoot)
                {
                    deletingObject -= value;
                }
            }

        }
#else
        public event EventHandler<DeletingEventsArgs> DeletingObject;
#endif


        /// <summary>
        ///     Raised after an object is deleted from database
        /// </summary>

#if UNITY3D
        private EventHandler<DeletedEventsArgs> deletedObject;
        public event EventHandler<DeletedEventsArgs> DeletedObject
        {
            add
            {
                lock (_syncRoot)
                {
                    deletedObject += value;
                }
            }
            remove
            {
                lock (_syncRoot)
                {
                    deletedObject -= value;
                }
            }

        }
#else
        public event EventHandler<DeletedEventsArgs> DeletedObject;
#endif


        /// <summary>
        ///     Raised before an object is loaded from database
        /// </summary>
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
#else
        public event EventHandler<LoadingObjectEventArgs> LoadingObject;
#endif


        /// <summary>
        ///     Raised after object is loaded from database
        /// </summary>
#if UNITY3D
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
#else
        public event EventHandler<LoadedObjectEventArgs> LoadedObject;
#endif

#if UNITY3D || CF || MONODROID
#else
        public event EventHandler<IndexesSaveAsyncFinishedArgs> IndexesSaveAsyncFinished;
#endif
        /// <summary>
        ///     Create a new instance of Siaqodb, database is not opened yet
        /// </summary>
        public Siaqodb()
        {
        }

        //TODO: add here WarningMessages and add for example Unoptimized queries
        /// <summary>
        ///     Create a new instance of Siaqodb and open the database
        /// </summary>
        /// <param name="path">Physical folder name where objects are stored</param>
#if !WinRT
        public Siaqodb(string path)
        {
            Open(path);
        }
#endif
#if SL4
       /// <summary>
        ///Create a new instance of Siaqodb, open database for OOB mode
       /// </summary>
       /// <param name="folderName">database folder name</param>
       /// <param name="specialFolder">special folder name for OOB mode ex.:MyDocuments, MyPictures, etc</param>
        public Siaqodb(string folderName,Environment.SpecialFolder specialFolder)
        {
           
            this.Open(folderName,specialFolder);
        }
#endif

#if !WinRT
        internal Siaqodb(string path, bool cacheTypes)
        {
            opened = true;
            this.path = path;
            storageEngine = new StorageEngine(this.path);
            indexManager = new IndexManager(this);
            storageEngine.indexManager = indexManager;

            storageEngine.NeedSaveComplexObject += storageEngine_NeedSaveComplexObject;
#if ASYNC
            storageEngine.NeedSaveComplexObjectAsync += storageEngine_NeedSaveComplexObjectAsync;
#endif

            storageEngine.LoadingObject += storageEngine_LoadingObject;
            storageEngine.LoadedObject += storageEngine_LoadedObject;
#if UNITY3D || CF || MONODROID
#else

            storageEngine.IndexesSaveAsyncFinished += storageEngine_IndexesSaveAsyncFinished;
#endif
            metaCache = new MetaCache();
            storageEngine.metaCache = metaCache;
            storageEngine.LoadMetaDataTypesForManager();
            cacheForManager = new CacheForManager();
        }
#endif

        internal Siaqodb(string path, string managerOption)
        {
            opened = true;
            this.path = path;

#if SILVERLIGHT
            storageEngine = new StorageEngine(this.path, true);
#else
            storageEngine = new StorageEngine(this.path);
#endif

            indexManager = new IndexManager(this);
            storageEngine.indexManager = indexManager;

            storageEngine.NeedSaveComplexObject += storageEngine_NeedSaveComplexObject;
#if ASYNC
            storageEngine.NeedSaveComplexObjectAsync += storageEngine_NeedSaveComplexObjectAsync;
#endif

            storageEngine.LoadedObject += storageEngine_LoadedObject;
            storageEngine.LoadingObject += storageEngine_LoadingObject;

            metaCache = new MetaCache();
            storageEngine.metaCache = metaCache;

            storageEngine.LoadAllTypes();
            var typesForIndexes = metaCache.DumpAllTypes();
            indexManager.BuildAllIndexes(typesForIndexes);

            RecoverAfterCrash();
            cacheForManager = new CacheForManager();
        }
#if !WinRT
        /// <summary>
        ///     Open database folder
        /// </summary>
        /// <param name="path">path where objects are stored</param>
        public void Open(string path)
        {
            opened = true;
            this.path = path;
            metaCache = new MetaCache();
            storageEngine = new StorageEngine(this.path);
            indexManager = new IndexManager(this);
            storageEngine.indexManager = indexManager;

            storageEngine.metaCache = metaCache;
            storageEngine.NeedSaveComplexObject += storageEngine_NeedSaveComplexObject;
#if ASYNC
            storageEngine.NeedSaveComplexObjectAsync += storageEngine_NeedSaveComplexObjectAsync;
#endif
            storageEngine.LoadingObject += storageEngine_LoadingObject;
            storageEngine.LoadedObject += storageEngine_LoadedObject;
#if UNITY3D || CF || MONODROID
#else
            storageEngine.IndexesSaveAsyncFinished += storageEngine_IndexesSaveAsyncFinished;
#endif
            storageEngine.LoadAllTypes();
            var typesForIndexes = metaCache.DumpAllTypes();
            indexManager.BuildAllIndexes(typesForIndexes);
            RecoverAfterCrash();
            cacheForManager = new CacheForManager();
        }
#if ASYNC
        public async Task OpenAsync(string path)
        {
            opened = true;
            this.path = path;
            metaCache = new MetaCache();
            storageEngine = new StorageEngine(this.path);
            indexManager = new IndexManager(this);
            storageEngine.indexManager = indexManager;

            storageEngine.metaCache = metaCache;
            storageEngine.NeedSaveComplexObject += storageEngine_NeedSaveComplexObject;
            storageEngine.NeedSaveComplexObjectAsync += storageEngine_NeedSaveComplexObjectAsync;
            storageEngine.LoadingObject += storageEngine_LoadingObject;
            storageEngine.LoadedObject += storageEngine_LoadedObject;
#if UNITY3D || CF || MONODROID
#else
            storageEngine.IndexesSaveAsyncFinished += storageEngine_IndexesSaveAsyncFinished;
#endif
            await storageEngine.LoadAllTypesAsync().ConfigureAwait(false);
            var typesForIndexes = metaCache.DumpAllTypes();
            await indexManager.BuildAllIndexesAsync(typesForIndexes);
            RecoverAfterCrash();
            cacheForManager = new CacheForManager();
        }
#endif
#endif
#if WinRT
        /// <summary>
        /// Open database folder
        /// </summary>
        /// <param name="databaseFolder">path where objects are stored</param>
        public async Task OpenAsync(StorageFolder databaseFolder)
        {

            this.opened = true;
            this.databaseFolder = databaseFolder;
            this.path = databaseFolder.Path;
            this.metaCache = new MetaCache();
            storageEngine = new StorageEngine(this.databaseFolder);
            indexManager = new IndexManager(this);
            storageEngine.indexManager = indexManager;
            storageEngine.metaCache = this.metaCache;
            storageEngine.NeedSaveComplexObject +=
 new EventHandler<Core.ComplexObjectEventArgs>(storageEngine_NeedSaveComplexObject);
            storageEngine.NeedSaveComplexObjectAsync += storageEngine_NeedSaveComplexObjectAsync;
            storageEngine.LoadingObject += new EventHandler<LoadingObjectEventArgs>(storageEngine_LoadingObject);
            storageEngine.LoadedObject += new EventHandler<LoadedObjectEventArgs>(storageEngine_LoadedObject);

            await storageEngine.LoadAllTypesAsync();
            List<SqoTypeInfo> typesForIndexes = this.metaCache.DumpAllTypes();
            await this.indexManager.BuildAllIndexesAsync(typesForIndexes);

            await this.RecoverAfterCrashAsync();
            cacheForManager = new sqoDB.Cache.CacheForManager();

            
        }

        public void Open(string dbpath)
        {
            var folder = StorageFolder.GetFolderFromPathAsync(dbpath).GetResults();
            this.Open(folder);
        }

        /// <summary>
        /// Open database folder
        /// </summary>
        /// <param name="databaseFolder">path where objects are stored</param>
        public void Open(StorageFolder databaseFolder)
        {

            this.opened = true;
            this.databaseFolder = databaseFolder;
            this.path = databaseFolder.Path;
            this.metaCache = new MetaCache();
            storageEngine = new StorageEngine(this.databaseFolder);
            indexManager = new IndexManager(this);
            storageEngine.indexManager = indexManager;
            storageEngine.metaCache = this.metaCache;
            storageEngine.NeedSaveComplexObject +=
 new EventHandler<Core.ComplexObjectEventArgs>(storageEngine_NeedSaveComplexObject);
            storageEngine.NeedSaveComplexObjectAsync += storageEngine_NeedSaveComplexObjectAsync;
            storageEngine.LoadingObject += new EventHandler<LoadingObjectEventArgs>(storageEngine_LoadingObject);
            storageEngine.LoadedObject += new EventHandler<LoadedObjectEventArgs>(storageEngine_LoadedObject);

            storageEngine.LoadAllTypes();
            List<SqoTypeInfo> typesForIndexes = this.metaCache.DumpAllTypes();
            this.indexManager.BuildAllIndexes(typesForIndexes);

            this.RecoverAfterCrash();
            cacheForManager = new sqoDB.Cache.CacheForManager();


        }
#endif
#if SL4
        /// <summary>
        /// Open database
        /// </summary>
        /// <param name="folderName">the name of folder where datafiles will be saved</param>
        /// <param name="specialFolder">special folder for OOB mode,ex:MyDocuments,MyPictures etc</param>
        public void Open(string folderName, Environment.SpecialFolder specialFolder)
        {
            string specF = Environment.GetFolderPath(specialFolder);
            if (specF == null)
            {
                throw new SiaqodbException("Siaqodb can run in OOB mode only if specialFolder is set");
            }
            string path = specF + System.IO.Path.DirectorySeparatorChar + folderName;

            this.opened = true;
            this.path = path;
            this.metaCache = new MetaCache();
            storageEngine = new StorageEngine(this.path,true);
            indexManager = new IndexManager(this);
            storageEngine.indexManager = indexManager;
            storageEngine.metaCache = this.metaCache;
            storageEngine.NeedSaveComplexObject +=
 new EventHandler<Core.ComplexObjectEventArgs>(storageEngine_NeedSaveComplexObject);
            storageEngine.LoadingObject += new EventHandler<LoadingObjectEventArgs>(storageEngine_LoadingObject);
            storageEngine.LoadedObject += new EventHandler<LoadedObjectEventArgs>(storageEngine_LoadedObject);

            storageEngine.LoadAllTypes();
            List<SqoTypeInfo> typesForIndexes = this.metaCache.DumpAllTypes();
            this.indexManager.BuildAllIndexes(typesForIndexes);
            this.RecoverAfterCrash();
            cacheForManager = new sqoDB.Cache.CacheForManager();
        }


#endif

        #region EVENTs_HND

#if UNITY3D || CF || MONODROID
#else
        private void storageEngine_IndexesSaveAsyncFinished(object sender, IndexesSaveAsyncFinishedArgs e)
        {
            OnIndexesSaveAsyncFinished(e);
        }
#endif
        private void storageEngine_LoadedObject(object sender, LoadedObjectEventArgs e)
        {
            OnLoadedObject(e);
        }

        private void storageEngine_LoadingObject(object sender, LoadingObjectEventArgs e)
        {
            OnLoadingObject(e);
        }

        private void storageEngine_NeedSaveComplexObject(object sender, ComplexObjectEventArgs e)
        {
            if (e.ComplexObject == null) return;
            var ti = GetSqoTypeInfoToStoreObject(e.ComplexObject);
            if (ti != null)
            {
                var oid = -1;
                if (e.ReturnOnlyOid_TID)
                {
                    oid = metaCache.GetOIDOfObject(e.ComplexObject, ti);
                }
                else if (circularRefCache.Contains(e.ComplexObject))
                {
                    oid = metaCache.GetOIDOfObject(e.ComplexObject, ti);
                }
                else
                {
                    circularRefCache.Add(e.ComplexObject);

                    oid = metaCache.GetOIDOfObject(e.ComplexObject, ti);
                    var inserted = oid == 0;
                    if (storeOnlyReferencesOfListItems && !inserted)
                    {
                        //skip save object and keep only reference
                    }
                    else
                    {
                        oid = storageEngine.SaveObject(e.ComplexObject, ti);
                    }

                    var saved = new SavedEventsArgs(e.ComplexObject.GetType(), e.ComplexObject);
                    saved.Inserted = inserted;
                    OnSavedObject(saved);
                }

                e.SavedOID = oid;
                e.TID = ti.Header.TID;
            }
        }
#if ASYNC
        private async Task storageEngine_NeedSaveComplexObjectAsync(object sender, ComplexObjectEventArgs e)
        {
            if (e.ComplexObject == null) return;
            var ti = await GetSqoTypeInfoToStoreObjectAsync(e.ComplexObject);
            if (ti != null)
            {
                var oid = -1;
                if (e.ReturnOnlyOid_TID)
                {
                    oid = metaCache.GetOIDOfObject(e.ComplexObject, ti);
                }
                else if (circularRefCache.Contains(e.ComplexObject))
                {
                    oid = metaCache.GetOIDOfObject(e.ComplexObject, ti);
                }
                else
                {
                    circularRefCache.Add(e.ComplexObject);

                    oid = metaCache.GetOIDOfObject(e.ComplexObject, ti);
                    var inserted = oid == 0;
                    if (storeOnlyReferencesOfListItems && !inserted)
                    {
                        //skip save object and keep only reference
                    }
                    else
                    {
                        oid = await storageEngine.SaveObjectAsync(e.ComplexObject, ti);
                    }

                    var saved = new SavedEventsArgs(e.ComplexObject.GetType(), e.ComplexObject);
                    saved.Inserted = inserted;
                    OnSavedObject(saved);
                }

                e.SavedOID = oid;
                e.TID = ti.Header.TID;
            }
        }
#endif
#if UNITY3D
        protected virtual void OnSavingObject(SavingEventsArgs e)
		{
			if (savingObject != null)
			{
				 if ((e.ObjectType.IsGenericType() && e.ObjectType.GetGenericTypeDefinition() == typeof(Indexes.BTreeNode<>)) || e.ObjectType == typeof(sqoDB.Indexes.IndexInfo2))
               {}
else
{
				savingObject(this, e);
}
			}
		}
		protected virtual void OnSavedObject(SavedEventsArgs e)
		{
			if (savedObject != null)
			{
 if ((e.ObjectType.IsGenericType() && e.ObjectType.GetGenericTypeDefinition() == typeof(Indexes.BTreeNode<>)) || e.ObjectType == typeof(sqoDB.Indexes.IndexInfo2))
               {}
else
{				
savedObject(this, e);
}
			}
		}
		protected virtual void OnDeletingObject(DeletingEventsArgs e)
		{
			if (deletingObject != null)
			{
				deletingObject(this, e);
			}
		}
		protected virtual void OnDeletedObject(DeletedEventsArgs e)
		{
			if (deletedObject != null)
			{
				deletedObject(this, e);
			}
		}
        protected virtual void OnLoadingObject(LoadingObjectEventArgs e)
        {
            if (loadingObject != null)
            {
                loadingObject(this, e);
            }
        }
        protected virtual void OnLoadedObject(LoadedObjectEventArgs e)
        {
            if (loadedObject != null)
            {
                loadedObject(this, e);
            }
        }
#else
        protected virtual void OnSavingObject(SavingEventsArgs e)
        {
            if (SavingObject != null)
            {
                if ((e.ObjectType.IsGenericType() && e.ObjectType.GetGenericTypeDefinition() == typeof(BTreeNode<>)) ||
                    e.ObjectType == typeof(IndexInfo2))
                {
                }
                else
                {
                    SavingObject(this, e);
                }
            }
        }

        protected virtual void OnSavedObject(SavedEventsArgs e)
        {
            if (SavedObject != null)
            {
                if ((e.ObjectType.IsGenericType() && e.ObjectType.GetGenericTypeDefinition() == typeof(BTreeNode<>)) ||
                    e.ObjectType == typeof(IndexInfo2))
                {
                }
                else
                {
                    SavedObject(this, e);
                }
            }
        }

        protected virtual void OnDeletingObject(DeletingEventsArgs e)
        {
            if (DeletingObject != null) DeletingObject(this, e);
        }

        protected virtual void OnDeletedObject(DeletedEventsArgs e)
        {
            if (DeletedObject != null) DeletedObject(this, e);
        }

        protected virtual void OnLoadingObject(LoadingObjectEventArgs e)
        {
            if (LoadingObject != null) LoadingObject(this, e);
        }

        protected virtual void OnLoadedObject(LoadedObjectEventArgs e)
        {
            if (LoadedObject != null) LoadedObject(this, e);
        }

#endif
#if UNITY3D || CF || MONODROID
#else
        protected virtual void OnIndexesSaveAsyncFinished(IndexesSaveAsyncFinishedArgs e)
        {
            if (IndexesSaveAsyncFinished != null) IndexesSaveAsyncFinished(this, e);
        }
#endif

        #endregion

        /// <summary>
        ///     Insert or update object; if object is loaded from database and this method is called then update will occur, if
        ///     object is new created then insert will occur
        /// </summary>
        /// <param name="obj">Object to be stored</param>
        public void StoreObject(object obj)
        {
            lock (_locker)
            {
                var ti = GetSqoTypeInfoToStoreObject(obj);
                if (ti != null)
                {
                    if ((ti.Type.IsGenericType() && ti.Type.GetGenericTypeDefinition() == typeof(BTreeNode<>)) ||
                        ti.Type == typeof(IndexInfo2))
                    {
                    }
                    else
                    {
                        circularRefCache.Clear();
                    }

                    circularRefCache.Add(obj);
                    var inserted = false;
#if UNITY3D
                    if (this.savedObject != null)//optimization 
                    {
                        inserted = metaCache.GetOIDOfObject(obj, ti) == 0;
                    }
#else
                    if (SavedObject != null) //optimization 
                        inserted = metaCache.GetOIDOfObject(obj, ti) == 0;
#endif
                    storageEngine.SaveObject(obj, ti);
                    var saved = new SavedEventsArgs(obj.GetType(), obj);
                    saved.Inserted = inserted;
                    OnSavedObject(saved);
                }
            }
        }
#if ASYNC
        /// <summary>
        ///     Insert or update object; if object is loaded from database and this method is called then update will occur, if
        ///     object is new created then insert will occur
        /// </summary>
        /// <param name="obj">Object to be stored</param>
        public async Task StoreObjectAsync(object obj)
        {
            var locked = false;
            await _lockerAsync.LockAsync(obj.GetType(), out locked);

            try
            {
                var ti = await GetSqoTypeInfoToStoreObjectAsync(obj);
                if (ti != null)
                {
                    if ((ti.Type.IsGenericType() && ti.Type.GetGenericTypeDefinition() == typeof(BTreeNode<>)) ||
                        ti.Type == typeof(IndexInfo2))
                    {
                    }
                    else
                    {
                        circularRefCache.Clear();
                    }

                    circularRefCache.Add(obj);
                    var inserted = false;
#if UNITY3D
                    if (this.savedObject != null)//optimization 
                    {
                        inserted = metaCache.GetOIDOfObject(obj, ti) == 0;
                    }
#else
                    if (SavedObject != null) //optimization 
                        inserted = metaCache.GetOIDOfObject(obj, ti) == 0;
#endif
                    await storageEngine.SaveObjectAsync(obj, ti);
                    var saved = new SavedEventsArgs(obj.GetType(), obj);
                    saved.Inserted = inserted;
                    OnSavedObject(saved);
                }
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        /// <summary>
        ///     Insert or update object partially, only provided properties are saved
        /// </summary>
        /// <param name="obj">object of which properties will be stored</param>
        /// <param name="properties">properties to be stored</param>
        public void StoreObjectPartially(object obj, params string[] properties)
        {
            StoreObjectPartially(obj, false, properties);
        }
#if ASYNC
        /// <summary>
        ///     Insert or update object partially, only provided properties are saved
        /// </summary>
        /// <param name="obj">object of which properties will be stored</param>
        /// <param name="properties">properties to be stored</param>
        public async Task StoreObjectPartiallyAsync(object obj, params string[] properties)
        {
            await StoreObjectPartiallyAsync(obj, false, properties);
        }
#endif
        /// <summary>
        ///     Insert or update object partially, only provided properties are saved
        /// </summary>
        /// <param name="obj">object of which properties will be stored</param>
        /// <param name="properties">properties to be stored</param>
        /// <param name="onlyReferences">if true,it will store only references to complex objects</param>
        public void StoreObjectPartially(object obj, bool onlyReferences, params string[] properties)
        {
            lock (_locker)
            {
                storeOnlyReferencesOfListItems = onlyReferences;
                var ti = GetSqoTypeInfoToStoreObject(obj);
                if (ti != null)
                {
                    circularRefCache.Clear();
                    circularRefCache.Add(obj);
                    storageEngine.SaveObjectPartially(obj, ti, properties);
                }

                storeOnlyReferencesOfListItems = false;
            }
        }
#if ASYNC
        /// <summary>
        ///     Insert or update object partially, only provided properties are saved
        /// </summary>
        /// <param name="obj">object of which properties will be stored</param>
        /// <param name="properties">properties to be stored</param>
        /// <param name="onlyReferences">if true,it will store only references to complex objects</param>
        public async Task StoreObjectPartiallyAsync(object obj, bool onlyReferences, params string[] properties)
        {
            var locked = false;
            await _lockerAsync.LockAsync(obj.GetType(), out locked);
            try
            {
                storeOnlyReferencesOfListItems = onlyReferences;
                var ti = await GetSqoTypeInfoToStoreObjectAsync(obj);
                if (ti != null)
                {
                    circularRefCache.Clear();
                    circularRefCache.Add(obj);
                    await storageEngine.SaveObjectPartiallyAsync(obj, ti, properties);
                }

                storeOnlyReferencesOfListItems = false;
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        /// <summary>
        ///     Insert or update object by a Transaction; if object is loaded from database and this method is called then update
        ///     will occur, if object is new created then insert will occur
        /// </summary>
        /// <param name="obj">Object to be stored</param>
        /// <param name="transaction">Transaction object</param>
        public void StoreObject(object obj, ITransaction transaction)
        {
            lock (_locker)
            {
                if (transaction == null) throw new ArgumentNullException("transaction");
                var ti = GetSqoTypeInfoToStoreObject(obj);
                if (ti != null)
                {
                    if (((Transaction)transaction).status == TransactionStatus.Closed)
                        throw new SiaqodbException("Transaction closed!");
                    //circularRefCache.Clear();
                    //circularRefCache.Add(obj); 

                    //circularRefCache is filled with obj just before Commit in TransactionManager, so not need to be added here
                    storageEngine.SaveObject(obj, ti, null, (Transaction)transaction);

                    var saved = new SavedEventsArgs(obj.GetType(), obj);
                    OnSavedObject(saved);
                }
            }
        }
#if ASYNC
        /// <summary>
        ///     Insert or update object by a Transaction; if object is loaded from database and this method is called then update
        ///     will occur, if object is new created then insert will occur
        /// </summary>
        /// <param name="obj">Object to be stored</param>
        /// <param name="transaction">Transaction object</param>
        public async Task StoreObjectAsync(object obj, ITransaction transaction)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");
            var locked = false;
            await _lockerAsync.LockAsync(obj.GetType(), out locked);
            try
            {
                var ti = await GetSqoTypeInfoToStoreObjectAsync(obj);
                if (ti != null)
                {
                    if (((Transaction)transaction).status == TransactionStatus.Closed)
                        throw new SiaqodbException("Transaction closed!");
                    //circularRefCache.Clear();
                    //circularRefCache.Add(obj); 

                    //circularRefCache is filled with obj just before Commit in TransactionManager, so not need to be added here
                    await storageEngine.SaveObjectAsync(obj, ti, null, (Transaction)transaction);

                    var saved = new SavedEventsArgs(obj.GetType(), obj);
                    OnSavedObject(saved);
                }
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif

        private SqoTypeInfo GetSqoTypeInfoToStoreObject(object obj)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            var objType = obj.GetType();
            var ev = new SavingEventsArgs(objType, obj);

            OnSavingObject(ev);
            if (ev.Cancel) return null;

            return GetSqoTypeInfoToStoreObject(obj.GetType());
        }
#if ASYNC
        private async Task<SqoTypeInfo> GetSqoTypeInfoToStoreObjectAsync(object obj)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            var objType = obj.GetType();
            var ev = new SavingEventsArgs(objType, obj);

            OnSavingObject(ev);
            if (ev.Cancel) return null;

            return await GetSqoTypeInfoToStoreObjectAsync(obj.GetType());
        }
#endif
        private SqoTypeInfo GetSqoTypeInfoToStoreObject(Type objType)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            SqoTypeInfo ti = null;


            if (metaCache.Contains(objType))
            {
                ti = metaCache.GetSqoTypeInfo(objType);
            }
            else
            {
                ti = MetaExtractor.GetSqoTypeInfo(objType);
                storageEngine.SaveType(ti);
                metaCache.AddType(objType, ti);
                indexManager.BuildIndexes(ti);
            }

            if (ti.IsOld)
                throw new TypeChangedException("Actual runtime Type:" + ti.Type.Name +
                                               "is different than Type stored in DB, in current version is not supported automatically type changing, to fix this, modify your class like it was when u saved objects in DB");
            return ti;
        }
#if ASYNC
        private async Task<SqoTypeInfo> GetSqoTypeInfoToStoreObjectAsync(Type objType)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            SqoTypeInfo ti = null;


            if (metaCache.Contains(objType))
            {
                ti = metaCache.GetSqoTypeInfo(objType);
            }
            else
            {
                ti = MetaExtractor.GetSqoTypeInfo(objType);
                await storageEngine.SaveTypeAsync(ti);
                metaCache.AddType(objType, ti);
                await indexManager.BuildIndexesAsync(ti);
            }

            if (ti.IsOld)
                throw new TypeChangedException("Actual runtime Type:" + ti.Type.Name +
                                               "is different than Type stored in DB, in current version is not supported automatically type changing, to fix this, modify your class like it was when u saved objects in DB");
            return ti;
        }
#endif
        /// <summary>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal IObjectList<T> Load<T>(Expression expression)
        {
            lock (_locker)
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();

                var oids = LoadOids<T>(expression);
                return storageEngine.LoadByOIDs<T>(oids, ti);
            }
        }
#if ASYNC
        /// <summary>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal async Task<IObjectList<T>> LoadAsync<T>(Expression expression)
        {
            var locked = false;
            await _lockerAsync.LockAsync(typeof(T), out locked);
            SqoTypeInfo ti = null;
            try
            {
                ti = CheckDBAndGetSqoTypeInfo<T>();
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }

            var oids = await LoadOidsAsync<T>(expression);

            locked = false;
            await _lockerAsync.LockAsync(typeof(T), out locked);
            try
            {
                return await storageEngine.LoadByOIDsAsync<T>(oids, ti);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        //public IObjectList<T> Objects<T>()
        //{
        //    return this.LoadAll<T>();
        //}
        /// <summary>
        ///     Load all objects of Type provided
        /// </summary>
        /// <typeparam name="T">Type of objects to be loaded from database</typeparam>
        /// <returns>List of objects retrieved from database</returns>
        public IObjectList<T> LoadAll<T>()
        {
            lock (_locker)
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                return storageEngine.LoadAll<T>(ti);
            }
        }
#if ASYNC
        /// <summary>
        ///     Load all objects of Type provided
        /// </summary>
        /// <typeparam name="T">Type of objects to be loaded from database</typeparam>
        /// <returns>List of objects retrieved from database</returns>
        public async Task<IObjectList<T>> LoadAllAsync<T>()
        {
            var locked = false;
            await _lockerAsync.LockAsync(typeof(T), out locked);
            try
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                return await storageEngine.LoadAllAsync<T>(ti);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        /// <summary>
        ///     Load object from database by OID provided
        /// </summary>
        /// <typeparam name="T">The Type of object to be loaded</typeparam>
        /// <param name="oid">oid of object</param>
        /// <returns>the object stored in database with oid provided</returns>
        public T LoadObjectByOID<T>(int oid)
        {
            lock (_locker)
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                return storageEngine.LoadObjectByOID<T>(ti, oid);
            }
        }
#if ASYNC
        /// <summary>
        ///     Load object from database by OID provided
        /// </summary>
        /// <typeparam name="T">The Type of object to be loaded</typeparam>
        /// <param name="oid">oid of object</param>
        /// <returns>the object stored in database with oid provided</returns>
        public async Task<T> LoadObjectByOIDAsync<T>(int oid)
        {
            var locked = false;
            await _lockerAsync.LockAsync(typeof(T), out locked);
            try
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                return await storageEngine.LoadObjectByOIDAsync<T>(ti, oid);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        internal T LoadObjectByOID<T>(int oid, List<string> properties)
        {
            lock (_locker)
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                return (T)storageEngine.LoadObjectByOID(ti, oid, properties);
            }
        }
#if ASYNC
        internal async Task<T> LoadObjectByOIDAsync<T>(int oid, List<string> properties)
        {
            bool locked;
            await _lockerAsync.LockAsync(typeof(T), out locked);
            try
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                return (T)await storageEngine.LoadObjectByOIDAsync(ti, oid, properties);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        /// <summary>
        ///     Close database
        /// </summary>
        public void Close()
        {
            lock (_locker)
            {
                opened = false;
                metaCache = null;
                storageEngine.Close();
                indexManager.Close();
            }
        }
#if ASYNC
        /// <summary>
        ///     Close database
        /// </summary>
        public async Task CloseAsync()
        {
            opened = false;
            metaCache = null;
            await storageEngine.CloseAsync();
            indexManager.Close();
        }
#endif
        /// <summary>
        ///     Flush buffered data to database
        /// </summary>
        public void Flush()
        {
            lock (_locker)
            {
                storageEngine.Flush();
            }
        }
#if ASYNC
        /// <summary>
        ///     Flush buffered data to database
        /// </summary>
        public async Task FlushAsync()
        {
            await _lockerAsync.LockAsync();
            try
            {
                await storageEngine.FlushAsync();
            }
            finally
            {
                _lockerAsync.Release();
            }
        }
#endif
        /// <summary>
        ///     Cast method to be used in LINQ queries
        /// </summary>
        /// <typeparam name="T">Type over which LINQ will take action</typeparam>
        /// <returns></returns>
        public ISqoQuery<T> Cast<T>()
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            return new SqoQuery<T>(this);
        }

        /// <summary>
        ///     Query method to be used in LINQ queries
        /// </summary>
        /// <typeparam name="T">Type over which LINQ will take action</typeparam>
        /// <returns></returns>
        public ISqoQuery<T> Query<T>()
        {
            return Cast<T>();
        }

        /// <summary>
        ///     Load OIDs by expression
        /// </summary>
        /// <typeparam name="T">Type for which OIDs will be loaded</typeparam>
        /// <param name="expression">filter expression</param>
        /// <returns>List of OIDs</returns>
        public List<int> LoadOids<T>(Expression expression)
        {
            lock (_locker)
            {
                if (expression == null) throw new ArgumentNullException("expression");
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                var t = new QueryTranslator(storageEngine, ti);
                var criteria = t.Translate(expression);
                return criteria.GetOIDs();
            }
        }
#if ASYNC
        /// <summary>
        ///     Load OIDs by expression
        /// </summary>
        /// <typeparam name="T">Type for which OIDs will be loaded</typeparam>
        /// <param name="expression">filter expression</param>
        /// <returns>List of OIDs</returns>
        public async Task<List<int>> LoadOidsAsync<T>(Expression expression)
        {
            var locked = false;
            await _lockerAsync.LockAsync(typeof(T), out locked);
            try
            {
                if (expression == null) throw new ArgumentNullException("expression");
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                var t = new QueryTranslator(storageEngine, ti);
                var criteria = t.Translate(expression);
                return await criteria.GetOIDsAsync();
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        internal List<int> LoadAllOIDs<T>()
        {
            lock (_locker)
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                return storageEngine.LoadAllOIDs(ti);
            }
        }
#if ASYNC
        internal async Task<List<int>> LoadAllOIDsAsync<T>()
        {
            var locked = false;
            await _lockerAsync.LockAsync(typeof(T), out locked);
            try
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                return await storageEngine.LoadAllOIDsAsync(ti);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif

        /// <summary>
        ///     Load all object OIDs of MetaType provided
        /// </summary>
        /// <param name="type">meta type Load by method GetAllTypes()</param>
        /// <returns></returns>
        public List<int> LoadAllOIDs(MetaType type)
        {
            lock (_locker)
            {
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
                return storageEngine.LoadAllOIDs(type.Name);
            }
        }
#if ASYNC
        /// <summary>
        ///     Load all object OIDs of MetaType provided
        /// </summary>
        /// <param name="type">meta type Load by method GetAllTypes()</param>
        /// <returns></returns>
        public async Task<List<int>> LoadAllOIDsAsync(MetaType type)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            return await storageEngine.LoadAllOIDsAsync(type.Name);
        }
#endif
        internal SqoTypeInfo GetSqoTypeInfo<T>()
        {
            var objType = typeof(T);
            return GetSqoTypeInfo(objType);
        }

        internal SqoTypeInfo GetSqoTypeInfo(Type t)
        {
            SqoTypeInfo ti = null;
            var objType = t;
            if (metaCache.Contains(objType))
                ti = metaCache.GetSqoTypeInfo(objType);
            else
                ti = MetaExtractor.GetSqoTypeInfo(objType);
            return ti;
        }


        internal List<KeyValuePair<int, int>> LoadOidsForJoin<TResult, TOuter, TInner>(SqoQuery<TOuter> outer,
            SqoQuery<TInner> inner, Expression outerExpression, Expression innerExpression)
        {
            var tiOuter = GetSqoTypeInfo<TOuter>();
            var tiInner = GetSqoTypeInfo<TInner>();

            var t = new JoinTranslator();
            var criteriaOuter = t.Translate(outerExpression);

            var criteriaInner = t.Translate(innerExpression);
            var oidOuter = outer.GetFilteredOids();
            var oidInner = inner.GetFilteredOids();

            var oidsPairs = storageEngine.LoadJoin(tiOuter, criteriaOuter, oidOuter, tiInner, criteriaInner, oidInner);

            return oidsPairs;
        }
#if ASYNC
        internal async Task<List<KeyValuePair<int, int>>> LoadOidsForJoinAsync<TResult, TOuter, TInner>(
            SqoQuery<TOuter> outer, SqoQuery<TInner> inner, Expression outerExpression, Expression innerExpression)
        {
            var tiOuter = GetSqoTypeInfo<TOuter>();
            var tiInner = GetSqoTypeInfo<TInner>();

            var t = new JoinTranslator();
            var criteriaOuter = t.Translate(outerExpression);

            var criteriaInner = t.Translate(innerExpression);
            var oidOuter = outer.GetFilteredOids();
            var oidInner = inner.GetFilteredOids();

            var oidsPairs =
                await storageEngine.LoadJoinAsync(tiOuter, criteriaOuter, oidOuter, tiInner, criteriaInner, oidInner);

            return oidsPairs;
        }

#endif


        internal object LoadValue(int oid, string fieldName, Type type)
        {
            var ti = GetSqoTypeInfo(type);
            return storageEngine.LoadValue(oid, fieldName, ti);
        }
#if ASYNC
        internal async Task<object> LoadValueAsync(int oid, string fieldName, Type type)
        {
            var ti = GetSqoTypeInfo(type);
            return await storageEngine.LoadValueAsync(oid, fieldName, ti);
        }
#endif
        /// <summary>
        ///     Load value of a field of an object identified by OID provided
        /// </summary>
        /// <param name="oid">OID of object</param>
        /// <param name="fieldName">fieldName</param>
        /// <param name="mt">MetaType</param>
        /// <returns></returns>
        public object LoadValue(int oid, string fieldName, MetaType mt)
        {
            lock (_locker)
            {
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
                if (!cacheForManager.Contains(mt.Name))
                {
                    var ti = storageEngine.GetSqoTypeInfo(mt.Name);
                    cacheForManager.AddType(mt.Name, ti);
                }

                var tinf = cacheForManager.GetSqoTypeInfo(mt.Name);
                return storageEngine.LoadValue(oid, fieldName, tinf);
            }
        }
#if ASYNC
        /// <summary>
        ///     Load value of a field of an object identified by OID provided
        /// </summary>
        /// <param name="oid">OID of object</param>
        /// <param name="fieldName">fieldName</param>
        /// <param name="mt">MetaType</param>
        /// <returns></returns>
        public async Task<object> LoadValueAsync(int oid, string fieldName, MetaType mt)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            if (!cacheForManager.Contains(mt.Name))
            {
                var ti = await storageEngine.GetSqoTypeInfoAsync(mt.Name);
                cacheForManager.AddType(mt.Name, ti);
            }

            var tinf = cacheForManager.GetSqoTypeInfo(mt.Name);
            var locked = false;
            await _lockerAsync.LockAsync(tinf.Type, out locked);
            try
            {
                return await storageEngine.LoadValueAsync(oid, fieldName, tinf);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        /// <summary>
        ///     Delete an object from database
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        public void Delete(object obj)
        {
            lock (_locker)
            {
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
                var t = obj.GetType();
                var ti = GetSqoTypeInfo(t);
                var deleted = DeleteObjInternal(obj, ti, null);
            }
        }
#if ASYNC
        /// <summary>
        ///     Delete an object from database
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        public async Task DeleteAsync(object obj)
        {
            var locked = false;
            await _lockerAsync.LockAsync(obj.GetType(), out locked);
            try
            {
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
                var t = obj.GetType();
                var ti = GetSqoTypeInfo(t);
                var deleted = await DeleteObjInternalAsync(obj, ti, null);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        /// <summary>
        ///     Delete an object from database using a Transaction
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        /// <param name="transaction">Transaction</param>
        public void Delete(object obj, ITransaction transaction)
        {
            lock (_locker)
            {
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");

                if (transaction == null) throw new ArgumentNullException("transaction");

                if (((Transaction)transaction).status == TransactionStatus.Closed)
                    throw new SiaqodbException("Transaction closed!");
                var t = obj.GetType();
                var ti = GetSqoTypeInfo(t);
                DeleteObjInternal(obj, ti, transaction);
            }
        }
#if ASYNC
        /// <summary>
        ///     Delete an object from database using a Transaction
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        /// <param name="transaction">Transaction</param>
        public async Task DeleteAsync(object obj, ITransaction transaction)
        {
            var locked = false;
            await _lockerAsync.LockAsync(obj.GetType(), out locked);
            try
            {
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
                if (transaction == null) throw new ArgumentNullException("transaction");
                if (((Transaction)transaction).status == TransactionStatus.Closed)
                    throw new SiaqodbException("Transaction closed!");
                var t = obj.GetType();
                var ti = GetSqoTypeInfo(t);
                await DeleteObjInternalAsync(obj, ti, transaction);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        /// <summary>
        ///     Delete an object from database by a certain field(ex:ID that come from server)
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        /// <param name="fieldName">Names of field that this method will lookup for object to delete it</param>
        public bool DeleteObjectBy(string fieldName, object obj)
        {
            return DeleteObjectBy(obj, fieldName);
        }
#if ASYNC
        /// <summary>
        ///     Delete an object from database by a certain field(ex:ID that come from server)
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        /// <param name="fieldName">Names of field that this method will lookup for object to delete it</param>
        public async Task<bool> DeleteObjectByAsync(string fieldName, object obj)
        {
            return await DeleteObjectByAsync(obj, fieldName);
        }
#endif
        /// <summary>
        ///     Delete an object from database by a certain field(ex:ID that come from server)
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        /// <param name="fieldNames">Names of fields that this method will lookup for object to delete it</param>
        public bool DeleteObjectBy(object obj, params string[] fieldNames)
        {
            return DeleteObjectBy(obj, null, fieldNames);
        }
#if ASYNC
        /// <summary>
        ///     Delete an object from database by a certain field(ex:ID that come from server)
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        /// <param name="fieldNames">Names of fields that this method will lookup for object to delete it</param>
        public async Task<bool> DeleteObjectByAsync(object obj, params string[] fieldNames)
        {
            return await DeleteObjectByAsync(obj, null, fieldNames);
        }
#endif
        /// <summary>
        ///     Delete an object from database by a certain field(ex:ID that come from server)
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        /// <param name="fieldNames">Names of fields that this method will lookup for object to delete it</param>
        /// <param name="transaction">Transaction object</param>
        public bool DeleteObjectBy(object obj, ITransaction transaction, params string[] fieldNames)
        {
            lock (_locker)
            {
                if (fieldNames == null || fieldNames.Length == 0) throw new ArgumentNullException("fieldNames");
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");


                if (transaction != null)
                    if (((Transaction)transaction).status == TransactionStatus.Closed)
                        throw new SiaqodbException("Transaction closed!");

                var t = obj.GetType();
                var ti = GetSqoTypeInfo(t);
                var delEv = new DeletingEventsArgs(ti.Type, -1); //we don't know it
                OnDeletingObject(delEv);
                if (delEv.Cancel) return false;

                var OID_deleted = storageEngine.DeleteObjectBy(fieldNames, obj, ti, (Transaction)transaction);
                var deletedEv = new DeletedEventsArgs(ti.Type, OID_deleted);
                OnDeletedObject(deletedEv);

                return OID_deleted > 0;
            }
        }
#if ASYNC
        /// <summary>
        ///     Delete an object from database by a certain field(ex:ID that come from server)
        /// </summary>
        /// <param name="obj">Object to be deleted</param>
        /// <param name="fieldNames">Names of fields that this method will lookup for object to delete it</param>
        /// <param name="transaction">Transaction object</param>
        public async Task<bool> DeleteObjectByAsync(object obj, ITransaction transaction, params string[] fieldNames)
        {
            if (fieldNames == null || fieldNames.Length == 0) throw new ArgumentNullException("fieldNames");
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            var locked = false;
            await _lockerAsync.LockAsync(obj.GetType(), out locked);
            try
            {
                if (transaction != null)
                    if (((Transaction)transaction).status == TransactionStatus.Closed)
                        throw new SiaqodbException("Transaction closed!");
                var t = obj.GetType();
                var ti = GetSqoTypeInfo(t);
                var delEv = new DeletingEventsArgs(ti.Type, -1); //we don't know it
                OnDeletingObject(delEv);
                if (delEv.Cancel) return false;

                var OID_deleted =
                    await storageEngine.DeleteObjectByAsync(fieldNames, obj, ti, (Transaction)transaction);
                var deletedEv = new DeletedEventsArgs(ti.Type, OID_deleted);
                OnDeletedObject(deletedEv);

                return OID_deleted > 0;
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        /// <summary>
        ///     Delete an object from database by a criteria
        /// </summary>
        /// <param name="criteria">Pairs of fields-values to lookup for object to delete it</param>
        /// <returns>Number of objects deleted</returns>
        public int DeleteObjectBy(Type objectType, Dictionary<string, object> criteria)
        {
            lock (_locker)
            {
                if (criteria == null || criteria.Keys.Count == 0) throw new ArgumentNullException("criteria");
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");

                var ti = GetSqoTypeInfo(objectType);
                var delEv = new DeletingEventsArgs(ti.Type, -1); //we don't know it
                OnDeletingObject(delEv);
                if (delEv.Cancel) return 0;

                var oidsDeleted = storageEngine.DeleteObjectBy(ti, criteria);
                foreach (var oid in oidsDeleted)
                {
                    var deletedEv = new DeletedEventsArgs(ti.Type, oid);
                    OnDeletedObject(deletedEv);
                }

                return oidsDeleted.Count;
            }
        }

        /// <summary>
        ///     Delete an object from database by a criteria
        /// </summary>
        /// <param name="criteria">Pairs of fields-values to lookup for object to delete it</param>
        /// <returns>Number of objects deleted</returns>
        public int DeleteObjectBy<T>(Dictionary<string, object> criteria)
        {
            return DeleteObjectBy(typeof(T), criteria);
        }
#if ASYNC
        /// <summary>
        ///     Delete an object from database by a criteria
        /// </summary>
        /// <param name="criteria">Pairs of fields-values to lookup for object to delete it</param>
        /// <returns>Number of objects deleted</returns>
        public async Task<int> DeleteObjectByAsync(Type objectType, Dictionary<string, object> criteria)
        {
            if (criteria == null || criteria.Keys.Count == 0) throw new ArgumentNullException("criteria");
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            var locked = false;
            await _lockerAsync.LockAsync(objectType, out locked);
            try
            {
                var ti = GetSqoTypeInfo(objectType);
                var delEv = new DeletingEventsArgs(ti.Type, -1); //we don't know it
                OnDeletingObject(delEv);
                if (delEv.Cancel) return 0;

                var oidsDeleted = await storageEngine.DeleteObjectByAsync(ti, criteria);
                foreach (var oid in oidsDeleted)
                {
                    var deletedEv = new DeletedEventsArgs(ti.Type, oid);
                    OnDeletedObject(deletedEv);
                }

                return oidsDeleted.Count;
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }

        /// <summary>
        ///     Delete an object from database by a criteria
        /// </summary>
        /// <param name="criteria">Pairs of fields-values to lookup for object to delete it</param>
        /// <returns>Number of objects deleted</returns>
        public async Task<int> DeleteObjectByAsync<T>(Dictionary<string, object> criteria)
        {
            return await DeleteObjectByAsync(typeof(T), criteria);
        }
#endif

        /// <summary>
        ///     Delete all objects of Type provided
        /// </summary>
        /// <typeparam name="T">Type of objects to be deleted</typeparam>
        public void DropType<T>()
        {
            DropType(typeof(T));
        }
#if ASYNC
        /// <summary>
        ///     Delete all objects of Type provided
        /// </summary>
        /// <typeparam name="T">Type of objects to be deleted</typeparam>
        public async Task DropTypeAsync<T>()
        {
            await DropTypeAsync(typeof(T));
        }
#endif
        /// <summary>
        ///     Delete all objects of Type provided
        /// </summary>
        /// <param name="type">Type of objects to be deleted</param>
        /// >
        public void DropType(Type type)
        {
            DropType(type, false);
        }
#if ASYNC
        /// <summary>
        ///     Delete all objects of Type provided
        /// </summary>
        /// <param name="type">Type of objects to be deleted</param>
        /// >
        public async Task DropTypeAsync(Type type)
        {
            await DropTypeAsync(type, false);
        }
#endif
        /// <summary>
        ///     Delete all objects of Type provided
        /// </summary>
        /// <param name="type">Type of objects to be deleted</param>
        /// <param name="claimFreespace">
        ///     If this is TRUE all dynamic length data associated with objects will be marked as free and
        ///     Shrink method is able to free the space
        /// </param>
        public void DropType(Type type, bool claimFreespace)
        {
            lock (_locker)
            {
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
                var ti = GetSqoTypeInfo(type);
                storageEngine.DropType(ti, claimFreespace);
                indexManager.DropIndexes(ti, claimFreespace);
                metaCache.Remove(type);
            }
        }
#if ASYNC
        /// <summary>
        ///     Delete all objects of Type provided
        /// </summary>
        /// <param name="type">Type of objects to be deleted</param>
        /// <param name="claimFreespace">
        ///     If this is TRUE all dynamic length data associated with objects will be marked as free and
        ///     Shrink method is able to free the space
        /// </param>
        public async Task DropTypeAsync(Type type, bool claimFreespace)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            var ti = GetSqoTypeInfo(type);
            await storageEngine.DropTypeAsync(ti, claimFreespace);
            await indexManager.DropIndexesAsync(ti, claimFreespace);
            metaCache.Remove(type);
        }
#endif
        internal object LoadObjectByOID(Type type, int oid)
        {
            var ti = GetSqoTypeInfo(type);
            return storageEngine.LoadObjectByOID(ti, oid);
        }
#if ASYNC
        internal async Task<object> LoadObjectByOIDAsync(Type type, int oid)
        {
            var ti = GetSqoTypeInfo(type);
            return await storageEngine.LoadObjectByOIDAsync(ti, oid);
        }
#endif
        /// <summary>
        ///     Return all Types from database folder
        /// </summary>
        /// <returns>List of MetaType objects</returns>
        public List<MetaType> GetAllTypes()
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            var list = new List<MetaType>();
            var tiList = storageEngine.LoadAllTypesForObjectManager();
            foreach (var ti in tiList)
            {
                var mt = new MetaType();
                mt.Name = ti.TypeName;
                mt.TypeID = ti.Header.TID;
                mt.FileName = ti.FileNameForManager;
                foreach (var fi in ti.Fields)
                {
                    var mf = new MetaField();

                    mf.FieldType = fi.AttributeType;
                    mf.Name = fi.Name;
                    mt.Fields.Add(mf);
                }

                list.Add(mt);
            }

            return list;
        }
#if ASYNC
        /// <summary>
        ///     Return all Types from database folder
        /// </summary>
        /// <returns>List of MetaType objects</returns>
        public async Task<List<MetaType>> GetAllTypesAsync()
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            var list = new List<MetaType>();
            var tiList = await storageEngine.LoadAllTypesForObjectManagerAsync();
            foreach (var ti in tiList)
            {
                var mt = new MetaType();
                mt.Name = ti.TypeName;
                mt.TypeID = ti.Header.TID;
                mt.FileName = ti.FileNameForManager;
                foreach (var fi in ti.Fields)
                {
                    var mf = new MetaField();

                    mf.FieldType = fi.AttributeType;
                    mf.Name = fi.Name;
                    mt.Fields.Add(mf);
                }

                list.Add(mt);
            }

            return list;
        }
#endif
        /// <summary>
        ///     Return number of objects of Type provided
        /// </summary>
        /// <typeparam name="T">Type of objects</typeparam>
        /// <returns></returns>
        public int Count<T>()
        {
            lock (_locker)
            {
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
                var ti = GetSqoTypeInfo<T>();
                return storageEngine.Count(ti);
            }
        }
#if ASYNC
        /// <summary>
        ///     Return number of objects of Type provided
        /// </summary>
        /// <typeparam name="T">Type of objects</typeparam>
        /// <returns></returns>
        public async Task<int> CountAsync<T>()
        {
            var locked = false;
            await _lockerAsync.LockAsync(typeof(T), out locked);
            try
            {
                if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
                var ti = GetSqoTypeInfo<T>();
                return await storageEngine.CountAsync(ti);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
#if !UNITY3D
        /// <summary>
        ///     Export to XML all objects of Type provided from database
        /// </summary>
        /// <typeparam name="T">Type of objects to be exported</typeparam>
        /// <param name="writer">XmlWriter</param>
        public void ExportToXML<T>(XmlWriter writer)
        {
            var objects = LoadAll<T>();
            ExportToXML(writer, objects);
        }

        /// <summary>
        ///     Export to XML list of objects provided
        /// </summary>
        /// <typeparam name="T">Type of objects</typeparam>
        /// <param name="writer">XmlWriter</param>
        /// <param name="objects">list of objects to be exported</param>
        public void ExportToXML<T>(XmlWriter writer, IList<T> objects)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            if (writer == null) throw new ArgumentNullException();
            if (objects == null) throw new ArgumentNullException();
            ImportExport.ExportToXML(writer, objects, this);
        }

        /// <summary>
        ///     Import from XML objects and return a list of them
        /// </summary>
        /// <typeparam name="T">Type of objects to be imported</typeparam>
        /// <param name="reader">XmlReader</param>
        /// <returns>List of objects imported</returns>
        public IObjectList<T> ImportFromXML<T>(XmlReader reader)
        {
            if (reader == null) throw new ArgumentNullException();
            return ImportExport.ImportFromXML<T>(reader, this);
        }

        /// <summary>
        ///     Import from XML objects and return a list and save into database
        /// </summary>
        /// <typeparam name="T">Type of objects to be imported</typeparam>
        /// <param name="reader">XmlReader</param>
        /// <param name="importIntoDB">if TRUE objects are saved also in database</param>
        /// <returns>List of objects imported</returns>
        public IObjectList<T> ImportFromXML<T>(XmlReader reader, bool importIntoDB)
        {
            var objects = ImportFromXML<T>(reader);
            if (importIntoDB)
                foreach (var o in objects)
                    StoreObject(o);
            return objects;
        }
#endif


        /// <summary>
        ///     Update an object in database by a certain Field(eq: ID that come from a server)
        /// </summary>
        /// <param name="fieldName">FieldName by which update is made(eq an ID)</param>
        /// <param name="obj">object that has all values but not OID to update it in database</param>
        /// <returns>true if object was updated and false if object was not found in database</returns>
        public bool UpdateObjectBy(string fieldName, object obj)
        {
            return UpdateObjectBy(obj, fieldName);
        }
#if ASYNC

        /// <summary>
        ///     Update an object in database by a certain Field(eq: ID that come from a server)
        /// </summary>
        /// <param name="fieldName">FieldName by which update is made(eq an ID)</param>
        /// <param name="obj">object that has all values but not OID to update it in database</param>
        /// <returns>true if object was updated and false if object was not found in database</returns>
        public async Task<bool> UpdateObjectByAsync(string fieldName, object obj)
        {
            return await UpdateObjectByAsync(obj, fieldName);
        }
#endif
        /// <summary>
        ///     Update an object in database by certain Fields(eq: ID that come from a server)
        /// </summary>
        /// <param name="fieldNames">name of fields by which update is made(eq an ID)</param>
        /// <param name="obj">object that has all values but not OID to update it in database</param>
        /// <returns>true if object was updated and false if object was not found in database</returns>
        public bool UpdateObjectBy(object obj, params string[] fieldNames)
        {
            return UpdateObjectBy(obj, null, fieldNames);
        }
#if ASYNC
        /// <summary>
        ///     Update an object in database by certain Fields(eq: ID that come from a server)
        /// </summary>
        /// <param name="fieldNames">name of fields by which update is made(eq an ID)</param>
        /// <param name="obj">object that has all values but not OID to update it in database</param>
        /// <returns>true if object was updated and false if object was not found in database</returns>
        public async Task<bool> UpdateObjectByAsync(object obj, params string[] fieldNames)
        {
            return await UpdateObjectByAsync(obj, null, fieldNames);
        }
#endif
        /// <summary>
        ///     Update an object in database by certain Fields(eq: ID that come from a server)
        /// </summary>
        /// <param name="fieldNames">name of fields by which update is made(eq an ID)</param>
        /// <param name="obj">object that has all values but not OID to update it in database</param>
        /// <param name="transaction">Transaction object</param>
        /// <returns>true if object was updated and false if object was not found in database</returns>
        public bool UpdateObjectBy(object obj, ITransaction transaction, params string[] fieldNames)
        {
            lock (_locker)
            {
                if (fieldNames == null || fieldNames.Length == 0) throw new ArgumentNullException("fieldsName");

                if (transaction != null)
                    if (((Transaction)transaction).status == TransactionStatus.Closed)
                        throw new SiaqodbException("Transaction closed!");


                var ti = GetSqoTypeInfoToStoreObject(obj);
                if (ti != null)
                {
                    var stored = storageEngine.UpdateObjectBy(fieldNames, obj, ti, (Transaction)transaction);

                    var saved = new SavedEventsArgs(obj.GetType(), obj);
                    saved.Inserted = false;
                    OnSavedObject(saved);

                    return stored;
                }

                return false;
            }
        }
#if ASYNC
        /// <summary>
        ///     Update an object in database by certain Fields(eq: ID that come from a server)
        /// </summary>
        /// <param name="fieldNames">name of fields by which update is made(eq an ID)</param>
        /// <param name="obj">object that has all values but not OID to update it in database</param>
        /// <param name="transaction">Transaction object</param>
        /// <returns>true if object was updated and false if object was not found in database</returns>
        public async Task<bool> UpdateObjectByAsync(object obj, ITransaction transaction, params string[] fieldNames)
        {
            var locked = false;
            await _lockerAsync.LockAsync(obj.GetType(), out locked);
            try
            {
                if (fieldNames == null || fieldNames.Length == 0) throw new ArgumentNullException("fieldsName");
                if (transaction != null)
                    if (((Transaction)transaction).status == TransactionStatus.Closed)
                        throw new SiaqodbException("Transaction closed!");

                var ti = await GetSqoTypeInfoToStoreObjectAsync(obj);
                if (ti != null)
                {
                    var stored = await storageEngine.UpdateObjectByAsync(fieldNames, obj, ti, (Transaction)transaction);

                    var saved = new SavedEventsArgs(obj.GetType(), obj);
                    saved.Inserted = false;
                    OnSavedObject(saved);

                    return stored;
                }

                return false;
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        internal bool UpdateField(int oid, MetaType metaType, string field, object value)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            if (!cacheForManager.Contains(metaType.Name))
            {
                var ti = storageEngine.GetSqoTypeInfo(metaType.Name);
                cacheForManager.AddType(metaType.Name, ti);
            }

            var tinf = cacheForManager.GetSqoTypeInfo(metaType.Name);

            return storageEngine.SaveValue(oid, field, tinf, value);
        }


        #region private methods

        private bool DeleteObjInternal(object obj, SqoTypeInfo ti, ITransaction transaction)
        {
            var oid = metaCache.GetOIDOfObject(obj, ti);
            if (oid <= 0 || oid > ti.Header.numberOfRecords)
                throw new SiaqodbException("Object not exists in database!");

            var delEv = new DeletingEventsArgs(ti.Type, oid);
            OnDeletingObject(delEv);
            if (delEv.Cancel) return false;
            if (transaction == null)
                storageEngine.DeleteObject(obj, ti);
            else
                storageEngine.DeleteObject(obj, ti, (Transaction)transaction, null);
            var deletedEv = new DeletedEventsArgs(ti.Type, oid);
            OnDeletedObject(deletedEv);
            return true;
        }
#if ASYNC
        private async Task<bool> DeleteObjInternalAsync(object obj, SqoTypeInfo ti, ITransaction transaction)
        {
            var oid = metaCache.GetOIDOfObject(obj, ti);
            if (oid <= 0 || oid > ti.Header.numberOfRecords)
                throw new SiaqodbException("Object not exists in database!");

            var delEv = new DeletingEventsArgs(ti.Type, oid);
            OnDeletingObject(delEv);
            if (delEv.Cancel) return false;
            if (transaction == null)
                await storageEngine.DeleteObjectAsync(obj, ti);
            else
                await storageEngine.DeleteObjectAsync(obj, ti, (Transaction)transaction, null);
            var deletedEv = new DeletedEventsArgs(ti.Type, oid);
            OnDeletedObject(deletedEv);
            return true;
        }
#endif
        internal SqoTypeInfo CheckDBAndGetSqoTypeInfo<T>()
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            var ti = GetSqoTypeInfo<T>();
            if (ti.IsOld)
                throw new TypeChangedException("Actual runtime Type:" + ti.Type.Name +
                                               "is different than Type stored in DB, in current version is not supported automatically type changing, to fix this, modify your class like it was when u saved objects in DB");
            return ti;
        }

        private SqoTypeInfo CheckDBAndGetSqoTypeInfo(Type type)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            var ti = GetSqoTypeInfo(type);
            if (ti.IsOld)
                throw new TypeChangedException("Actual runtime Type:" + ti.Type.Name +
                                               "is different than Type stored in DB, in current version is not supported automatically type changing, to fix this, modify your class like it was when u saved objects in DB");
            return ti;
        }

        #endregion


        internal List<object> LoadDirtyObjects(Type type)
        {
            lock (_locker)
            {
                var ti = CheckDBAndGetSqoTypeInfo(type);
                var w = new Where("isDirty", OperationType.Equal, true);
                w.StorageEngine = storageEngine;
                w.ParentSqoTypeInfo = ti;
                w.ParentType.Add(w.ParentSqoTypeInfo.Type);
                var oidsDirty = w.GetOIDs();

                var wDelete = new Where("isTombstone", OperationType.Equal, true);
                wDelete.StorageEngine = storageEngine;
                wDelete.ParentSqoTypeInfo = ti;
                wDelete.ParentType.Add(wDelete.ParentSqoTypeInfo.Type);
                var oidsDeleted = storageEngine.LoadFilteredDeletedOids(wDelete, ti);

                oidsDirty.AddRange(oidsDeleted);

                return storageEngine.LoadByOIDs(oidsDirty, ti);
            }
        }
#if ASYNC
        internal async Task<List<object>> LoadDirtyObjectsAsync(Type type)
        {
            var locked = false;
            await _lockerAsync.LockAsync(type, out locked);
            try
            {
                var ti = CheckDBAndGetSqoTypeInfo(type);
                var w = new Where("isDirty", OperationType.Equal, true);
                w.StorageEngine = storageEngine;
                w.ParentSqoTypeInfo = ti;
                w.ParentType.Add(w.ParentSqoTypeInfo.Type);
                var oidsDirty = await w.GetOIDsAsync();

                var wDelete = new Where("isTombstone", OperationType.Equal, true);
                wDelete.StorageEngine = storageEngine;
                wDelete.ParentSqoTypeInfo = ti;
                wDelete.ParentType.Add(wDelete.ParentSqoTypeInfo.Type);
                var oidsDeleted = await storageEngine.LoadFilteredDeletedOidsAsync(wDelete, ti);

                oidsDirty.AddRange(oidsDeleted);

                return await storageEngine.LoadByOIDsAsync(oidsDirty, ti);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif

        /// <summary>
        ///     return current database path
        /// </summary>
        /// <returns>The database folder path</returns>
        public string GetDBPath()
        {
            return path;
        }

        /// <summary>
        ///     Start a database Transaction to be used on insert/update/delete objects
        /// </summary>
        /// <returns> Transaction object</returns>
        public ITransaction BeginTransaction()
        {
            circularRefCache.Clear();
            return TransactionManager.BeginTransaction(this);
        }

        internal TransactionsStorage GetTransactionLogStorage()
        {
            return storageEngine.GetTransactionLogStorage();
        }

        internal void DropTransactionLog()
        {
            storageEngine.DropTransactionLog();
        }

        private void RecoverAfterCrash()
        {
            var ti = CheckDBAndGetSqoTypeInfo<TransactionObjectHeader>();

            var tiTypeHeader = CheckDBAndGetSqoTypeInfo<TransactionTypeHeader>();

            storageEngine.RecoverAfterCrash(ti, tiTypeHeader);
        }
#if ASYNC
        private async Task RecoverAfterCrashAsync()
        {
            var ti = CheckDBAndGetSqoTypeInfo<TransactionObjectHeader>();

            var tiTypeHeader = CheckDBAndGetSqoTypeInfo<TransactionTypeHeader>();

            await storageEngine.RecoverAfterCrashAsync(ti, tiTypeHeader);
        }
#endif
        internal void TransactionCommitStatus(bool started)
        {
            storageEngine.TransactionCommitStatus(started);
        }

        internal void Flush<T>()
        {
            var ti = CheckDBAndGetSqoTypeInfo<T>();
            storageEngine.Flush(ti);
        }
#if ASYNC
        internal async Task FlushAsync<T>()
        {
            var ti = CheckDBAndGetSqoTypeInfo<T>();
            await storageEngine.FlushAsync(ti);
        }
#endif

        internal void DeleteObjectByMeta(int oid, MetaType metaType)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            if (!cacheForManager.Contains(metaType.Name))
            {
                var ti = storageEngine.GetSqoTypeInfo(metaType.Name);
                cacheForManager.AddType(metaType.Name, ti);
            }

            var tinf = cacheForManager.GetSqoTypeInfo(metaType.Name);

            storageEngine.DeleteObjectByOID(oid, tinf);
        }

        internal int InsertObjectByMeta(MetaType metaType)
        {
            if (!opened) throw new SiaqodbException("Database is closed, call method Open() to open it!");
            if (!cacheForManager.Contains(metaType.Name))
            {
                var ti = storageEngine.GetSqoTypeInfo(metaType.Name);
                cacheForManager.AddType(metaType.Name, ti);
            }

            var tinf = cacheForManager.GetSqoTypeInfo(metaType.Name);
            return storageEngine.InsertObjectByMeta(tinf);
        }

        internal IBTree GetIndex(string field, Type type)
        {
            var ti = GetSqoTypeInfo(type);
            return indexManager.GetIndex(field, ti);
        }

        /// <summary>
        ///     Get a list of unique values for a field index
        /// </summary>
        /// <typeparam name="T">Type where index is defined</typeparam>
        /// <typeparam name="TIndex">Type of field indexed</typeparam>
        /// <param name="fieldName">Name of field or automatic property which is indexed</param>
        /// <returns></returns>
        public IList<TIndex> LoadIndexValues<T, TIndex>(string fieldName)
        {
            var fieldNameAsInDB = MetaHelper.GetFieldAsInDB(fieldName, typeof(T));
            var index = GetIndex(fieldNameAsInDB, typeof(T));
            if (index != null)
            {
                var indexT = (IBTree<TIndex>)index;
                return indexT.DumpKeys();
            }

            return new List<TIndex>();
        }

        /// <summary>
        ///     Load all objects in Lazy mode, objects are activated/read from db when it is accessed
        ///     by index or by enumerator
        /// </summary>
        /// <typeparam name="T">Type of objects to be loaded from database</typeparam>
        /// <returns>LazyObjectList of objects</returns>
        public IObjectList<T> LoadAllLazy<T>()
        {
            lock (_locker)
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                var oids = storageEngine.LoadAllOIDs(ti);
                return new LazyObjectList<T>(this, oids);
            }
        }
#if ASYNC
        /// <summary>
        ///     Load all objects in Lazy mode, objects are activated/read from db when it is accessed
        ///     by index or by enumerator
        /// </summary>
        /// <typeparam name="T">Type of objects to be loaded from database</typeparam>
        /// <returns>LazyObjectList of objects</returns>
        public async Task<IObjectList<T>> LoadAllLazyAsync<T>()
        {
            var locked = false;
            await _lockerAsync.LockAsync(typeof(T), out locked);
            try
            {
                var ti = CheckDBAndGetSqoTypeInfo<T>();
                var oids = await storageEngine.LoadAllOIDsAsync(ti);
                return new LazyObjectList<T>(this, oids);
            }
            finally
            {
                if (locked) _lockerAsync.Release();
            }
        }
#endif
        internal void LoadObjectOIDAndTID(int oid, string fieldName, MetaType mt, ref List<int> listOIDs, ref int TID)
        {
            if (!cacheForManager.Contains(mt.Name))
            {
                var ti = storageEngine.GetSqoTypeInfo(mt.Name);
                cacheForManager.AddType(mt.Name, ti);
            }

            var tinf = cacheForManager.GetSqoTypeInfo(mt.Name);
            var fi = MetaHelper.FindField(tinf.Fields, fieldName);
            if (fi.AttributeTypeId == MetaExtractor.complexID || fi.AttributeTypeId == MetaExtractor.documentID)
            {
                var kv = storageEngine.LoadOIDAndTID(oid, fi, tinf);
                listOIDs.Add(kv.Key);
                TID = kv.Value;
            }
            else if (fi.AttributeTypeId - MetaExtractor.ArrayTypeIDExtra == MetaExtractor.complexID)
            {
                var list = storageEngine.LoadComplexArray(oid, fi, tinf);
                if (list.Count > 0)
                {
                    TID = list[0].Value;
                    foreach (var kv in list) listOIDs.Add(kv.Key);
                }
            }
        }


        internal void LoadTIDofComplex(int oid, string fieldName, MetaType mt, ref int TID, ref bool isArray)
        {
            if (!cacheForManager.Contains(mt.Name))
            {
                var ti = storageEngine.GetSqoTypeInfo(mt.Name);
                cacheForManager.AddType(mt.Name, ti);
            }

            var tinf = cacheForManager.GetSqoTypeInfo(mt.Name);
            var fi = MetaHelper.FindField(tinf.Fields, fieldName);
            if (fi.AttributeTypeId == MetaExtractor.complexID || fi.AttributeTypeId == MetaExtractor.documentID)
            {
                var kv = storageEngine.LoadOIDAndTID(oid, fi, tinf);
                TID = kv.Value;
                isArray = false;
            }
            else if (fi.AttributeTypeId - MetaExtractor.ArrayTypeIDExtra == MetaExtractor.complexID)
            {
                isArray = true;
                TID = storageEngine.LoadComplexArrayTID(oid, fi, tinf);
            }
            else if (fi.AttributeTypeId - MetaExtractor.ArrayTypeIDExtra == MetaExtractor.jaggedArrayID)
            {
                isArray = true;
                TID = -32;
            }
            else if (fi.AttributeTypeId == MetaExtractor.dictionaryID)
            {
                TID = -31;
            }
        }

        public void StartBulkInsert(params Type[] types)
        {
            Monitor.Enter(_locker);
            foreach (var t in types)
            {
                var ti = GetSqoTypeInfoToStoreObject(t);
                if (ti != null) PutIndexPersiststenceState(ti, false);
            }
        }
#if ASYNC
        public async Task StartBulkInsertAsync(params Type[] types)
        {
            foreach (var t in types)
            {
                var ti = await GetSqoTypeInfoToStoreObjectAsync(t);
                if (ti != null) PutIndexPersiststenceState(ti, false);
            }
        }
#endif
        public void EndBulkInsert(params Type[] types)
        {
            foreach (var t in types)
            {
                var ti = GetSqoTypeInfoToStoreObject(t);
                if (ti != null)
                {
                    PutIndexPersiststenceState(ti, true);
                    PersistIndexDirtyNodes(ti);
                }
            }

            Monitor.Exit(_locker);
        }
#if ASYNC
        public async Task EndBulkInsertAsync(params Type[] types)
        {
            foreach (var t in types)
            {
                var ti = await GetSqoTypeInfoToStoreObjectAsync(t);
                if (ti != null)
                {
                    PutIndexPersiststenceState(ti, true);
                    PersistIndexDirtyNodes(ti);
                }
            }
        }
#endif

        #region Indexes

        internal bool IsObjectDeleted(int oid, SqoTypeInfo ti)
        {
            return storageEngine.IsObjectDeleted(oid, ti);
        }
#if ASYNC
        internal async Task<bool> IsObjectDeletedAsync(int oid, SqoTypeInfo ti)
        {
            return await storageEngine.IsObjectDeletedAsync(oid, ti);
        }
#endif
        internal void PutIndexPersiststenceState(SqoTypeInfo ti, bool on)
        {
            indexManager.PutIndexPersistenceOnOff(ti, on);
        }

        internal void PersistIndexDirtyNodes(SqoTypeInfo ti)
        {
            indexManager.Persist(ti);
        }
#if ASYNC
        internal async Task PersistIndexDirtyNodesAsync(SqoTypeInfo ti)
        {
            await indexManager.PersistAsync(ti);
        }
#endif

        #endregion

        internal int AllocateNewOID<T>()
        {
            var ti = GetSqoTypeInfoToStoreObject(typeof(T));
            if (ti != null) return storageEngine.AllocateNewOID(ti);
            return 0;
        }
#if ASYNC
        internal async Task<int> AllocateNewOIDAsync<T>()
        {
            var ti = await GetSqoTypeInfoToStoreObjectAsync(typeof(T));
            if (ti != null) return await storageEngine.AllocateNewOIDAsync(ti);
            return 0;
        }
#endif


        internal ISqoFile GetRawFile()
        {
            return storageEngine.GetRawFile();
        }

        internal void ReIndexAll(bool claimFreespace)
        {
            var typesForIndexes = metaCache.DumpAllTypes();
            foreach (var ti in typesForIndexes)
                if (ti.Type.IsGenericType() && ti.Type.GetGenericTypeDefinition() == typeof(BTreeNode<>))
                    DropType(ti.Type, claimFreespace);
            indexManager.DeleteAllIndexInfo();
            DropType(typeof(IndexInfo2));

            indexManager.BuildAllIndexes(typesForIndexes);
        }
#if ASYNC

        internal async Task ReIndexAllAsync(bool claimFreespace)
        {
            var typesForIndexes = metaCache.DumpAllTypes();
            foreach (var ti in typesForIndexes)
                if (ti.Type.IsGenericType() && ti.Type.GetGenericTypeDefinition() == typeof(BTreeNode<>))
                    await DropTypeAsync(ti.Type, claimFreespace);
            indexManager.DeleteAllIndexInfo();
            await DropTypeAsync(typeof(IndexInfo2));

            await indexManager.BuildAllIndexesAsync(typesForIndexes);
        }
#endif
        internal List<int> GetUsedRawdataInfoOIDS()
        {
            var existingTypes = metaCache.DumpAllTypes();
            var oids = new List<int>();
            foreach (var ti in existingTypes) oids.AddRange(storageEngine.GetUsedRawdataInfoOIDs(ti));
            return oids;
        }
#if ASYNC
        internal async Task<List<int>> GetUsedRawdataInfoOIDSAsync()
        {
            var existingTypes = metaCache.DumpAllTypes();
            var oids = new List<int>();
            foreach (var ti in existingTypes)
            {
                var l = await storageEngine.GetUsedRawdataInfoOIDsAsync(ti);
                oids.AddRange(l);
            }

            return oids;
        }
#endif
        internal void MarkRawInfoAsFree(List<int> rawdataInfoOIDs)
        {
            storageEngine.MarkRawInfoAsFree(rawdataInfoOIDs);
        }
#if ASYNC
        internal async Task MarkRawInfoAsFreeAsync(List<int> rawdataInfoOIDs)
        {
            await storageEngine.MarkRawInfoAsFreeAsync(rawdataInfoOIDs);
        }
#endif
        internal void RepairAllTypes()
        {
            var existingTypes = metaCache.DumpAllTypes();

            foreach (var ti in existingTypes)
            {
                var oids = storageEngine.LoadAllOIDs(ti);
                foreach (var oid in oids)
                {
                    var obj = storageEngine.LoadObjectByOID(ti, oid);
                }
            }
        }
#if ASYNC
        internal async Task RepairAllTypesAsync()
        {
            var existingTypes = metaCache.DumpAllTypes();

            foreach (var ti in existingTypes)
            {
                var oids = await storageEngine.LoadAllOIDsAsync(ti);
                foreach (var oid in oids)
                {
                    var obj = await storageEngine.LoadObjectByOIDAsync(ti, oid);
                }
            }
        }
#endif
        internal void ShrinkAllTypes()
        {
            lock (_locker)
            {
                var existingTypes = metaCache.DumpAllTypes();


                foreach (var ti in existingTypes)
                {
                    if (ti.Type == typeof(RawdataInfo) ||
                        ti.Type == typeof(IndexInfo2) ||
                        (ti.Type.IsGenericType() && ti.Type.GetGenericTypeDefinition() == typeof(BTreeNode<>)))
                        continue;

                    var oids = storageEngine.LoadAllOIDs(ti);
                    var objectBytes = new Dictionary<int, byte[]>();
                    foreach (var oid in oids)
                    {
                        var obj = storageEngine.GetObjectBytes(oid, ti);
                        objectBytes.Add(oid, obj);
                    }

                    storageEngine.SetFileLength(ti.Header.headerSize, ti);
                    ti.Header.numberOfRecords = 0;
                    if (oids.Count == 0) storageEngine.SaveType(ti); //to save nrRecords
                    foreach (var oid in objectBytes.Keys)
                    {
                        var newOID = storageEngine.SaveObjectBytes(objectBytes[oid], ti);
                        var shrinkResult = new ShrinkResult
                        {
                            Old_OID = oid,
                            New_OID = newOID,
                            TID = ti.Header.TID
                        };
                        StoreObject(shrinkResult);
                    }
                }

                IList<ShrinkResult> shrinkResults = LoadAll<ShrinkResult>();
                if (shrinkResults.Count > 0)
                    foreach (var ti in existingTypes)
                    {
                        if (ti.Type == typeof(RawdataInfo) ||
                            ti.Type == typeof(IndexInfo2) ||
                            (ti.Type.IsGenericType() && ti.Type.GetGenericTypeDefinition() == typeof(BTreeNode<>)))
                            continue;
                        storageEngine.AdjustComplexFieldsAfterShrink(ti, shrinkResults);
                    }

                DropType(typeof(ShrinkResult));
            }
        }
#if ASYNC
        internal async Task ShrinkAllTypesAsync()
        {
            var existingTypes = metaCache.DumpAllTypes();


            foreach (var ti in existingTypes)
            {
                if (ti.Type == typeof(RawdataInfo) ||
                    ti.Type == typeof(IndexInfo2) ||
                    (ti.Type.IsGenericType() && ti.Type.GetGenericTypeDefinition() == typeof(BTreeNode<>)))
                    continue;

                var oids = await storageEngine.LoadAllOIDsAsync(ti);
                var objectBytes = new Dictionary<int, byte[]>();
                foreach (var oid in oids)
                {
                    var obj = await storageEngine.GetObjectBytesAsync(oid, ti);
                    objectBytes.Add(oid, obj);
                }

                storageEngine.SetFileLength(ti.Header.headerSize, ti);
                ti.Header.numberOfRecords = 0;
                if (oids.Count == 0) await storageEngine.SaveTypeAsync(ti); //to save nrRecords
                foreach (var oid in objectBytes.Keys)
                {
                    var newOID = await storageEngine.SaveObjectBytesAsync(objectBytes[oid], ti);
                    var shrinkResult = new ShrinkResult
                    {
                        Old_OID = oid,
                        New_OID = newOID,
                        TID = ti.Header.TID
                    };
                    await StoreObjectAsync(shrinkResult);
                }
            }

            IList<ShrinkResult> shrinkResults = await LoadAllAsync<ShrinkResult>();
            if (shrinkResults.Count > 0)
                foreach (var ti in existingTypes)
                {
                    if (ti.Type == typeof(RawdataInfo) ||
                        ti.Type == typeof(IndexInfo2) ||
                        (ti.Type.IsGenericType() && ti.Type.GetGenericTypeDefinition() == typeof(BTreeNode<>)))
                        continue;
                    await storageEngine.AdjustComplexFieldsAfterShrinkAsync(ti, shrinkResults);
                }

            await DropTypeAsync(typeof(ShrinkResult));
        }

#endif
        /// <summary>
        ///     Get OID of object, if the Type of object has not defined OID property then object and OID are weak cached during
        ///     object load from database and this value is returned,
        ///     otherwise it is returned value of the OID property
        /// </summary>
        /// <param name="obj">The object for which OID is returned</param>
        /// <returns>The OID associated with object that is stored in database</returns>
        public int GetOID(object obj)
        {
            lock (_locker)
            {
                if (obj == null) throw new ArgumentNullException("obj");

                var ti = CheckDBAndGetSqoTypeInfo(obj.GetType());
                return metaCache.GetOIDOfObject(obj, ti);
            }
        }

        internal void SetDatabaseFileName(string fileName, MetaType type)
        {
            CacheCustomFileNames.AddFileNameForType(type.Name, fileName, false);
        }

        internal void GetOIDForAMSByField(object obj, string fieldName)
        {
            var ti = GetSqoTypeInfoToStoreObject(obj.GetType());
            var oids = storageEngine.LoadOidsByField(ti, fieldName, obj);

            if (oids.Count > 1)
                throw new SiaqodbException("Many objects with this field value exists is database.");
            if (oids.Count == 1) metaCache.SetOIDToObject(obj, oids[0], ti);
        }

        internal void ShrinkRawInfo()
        {
            Expression<Func<RawdataInfo, bool>> predicate = ri => ri.IsFree == false;
            var existingOIDsOccupied = LoadOids<RawdataInfo>(predicate);
            var tiRawInfo = GetSqoTypeInfo<RawdataInfo>();
            //dump object bytes
            var objectBytes = new Dictionary<int, byte[]>();
            foreach (var oid in existingOIDsOccupied)
            {
                var obj = storageEngine.GetObjectBytes(oid, tiRawInfo);
                objectBytes.Add(oid, obj);
            }

            //store objects with new OIDs
            storageEngine.SetFileLength(tiRawInfo.Header.headerSize, tiRawInfo);
            tiRawInfo.Header.numberOfRecords = 0;
            if (existingOIDsOccupied.Count == 0) storageEngine.SaveType(tiRawInfo); //to save nrRecords
            var oldNewOIDs = new Dictionary<int, int>();
            foreach (var oid in objectBytes.Keys)
            {
                var newOID = storageEngine.SaveObjectBytes(objectBytes[oid], tiRawInfo);
                oldNewOIDs.Add(oid, newOID);
            }

            if (oldNewOIDs.Keys.Count > 0)
            {
                var existingTypes = metaCache.DumpAllTypes();
                foreach (var ti in existingTypes)
                {
                    if (ti.Type == typeof(RawdataInfo)) continue;
                    var oldOIDs = storageEngine.GetUsedRawdataInfoOIDsAndFieldInfos(ti);
                    foreach (var oldRawInfoOID in oldOIDs.Keys)
                        if (oldNewOIDs.ContainsKey(oldRawInfoOID))
                        {
                            var newOID = oldNewOIDs[oldRawInfoOID];
                            storageEngine.AdjustArrayFieldsAfterShrink(ti, oldOIDs[oldRawInfoOID].Value,
                                oldOIDs[oldRawInfoOID].Name, newOID);
                        }
                }
            }
        }

        internal string GetFileName(Type type)
        {
            var ti = GetSqoTypeInfoToStoreObject(type);
            return storageEngine.GetFileName(ti);
        }
    }
}