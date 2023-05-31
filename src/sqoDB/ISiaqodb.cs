using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Xml;
using sqoDB.Transactions;

namespace sqoDB
{
    public interface ISiaqodb
    {
        ITransaction BeginTransaction();
        ISqoQuery<T> Cast<T>();
        void Close();
        int Count<T>();
        void Delete(object obj);
        void Delete(object obj, ITransaction transaction);
        event EventHandler<DeletedEventsArgs> DeletedObject;
        bool DeleteObjectBy(object obj, params string[] fieldNames);
        bool DeleteObjectBy(object obj, ITransaction transaction, params string[] fieldNames);
        bool DeleteObjectBy(string fieldName, object obj);
        int DeleteObjectBy(Type objectType, Dictionary<string, object> criteria);
        int DeleteObjectBy<T>(Dictionary<string, object> criteria);
        event EventHandler<DeletingEventsArgs> DeletingObject;
        void DropType(Type type);
        void DropType(Type type, bool claimFreespace);
        void DropType<T>();
        void EndBulkInsert(params Type[] types);
#if !UNITY3D
        void ExportToXML<T>(XmlWriter writer);
        void ExportToXML<T>(XmlWriter writer, IList<T> objects);
        IObjectList<T> ImportFromXML<T>(XmlReader reader);
        IObjectList<T> ImportFromXML<T>(XmlReader reader, bool importIntoDB);
#elif !CF && !UNITY3D
         event EventHandler<IndexesSaveAsyncFinishedArgs> IndexesSaveAsyncFinished;
#endif
        void Flush();
        List<MetaType> GetAllTypes();
        string GetDBPath();
        int GetOID(object obj);
        IObjectList<T> LoadAll<T>();
        IObjectList<T> LoadAllLazy<T>();
        List<int> LoadAllOIDs(MetaType type);
        event EventHandler<LoadedObjectEventArgs> LoadedObject;
        IList<TIndex> LoadIndexValues<T, TIndex>(string fieldName);
        event EventHandler<LoadingObjectEventArgs> LoadingObject;
        T LoadObjectByOID<T>(int oid);
        List<int> LoadOids<T>(Expression expression);
        object LoadValue(int oid, string fieldName, MetaType mt);
        ISqoQuery<T> Query<T>();
        event EventHandler<SavedEventsArgs> SavedObject;
        event EventHandler<SavingEventsArgs> SavingObject;
        void StartBulkInsert(params Type[] types);
        void StoreObject(object obj);
        void StoreObject(object obj, ITransaction transaction);
        void StoreObjectPartially(object obj, params string[] properties);
        void StoreObjectPartially(object obj, bool onlyReferences, params string[] properties);
        bool UpdateObjectBy(object obj, params string[] fieldNames);
        bool UpdateObjectBy(object obj, ITransaction transaction, params string[] fieldNames);
        bool UpdateObjectBy(string fieldName, object obj);

#if ASYNC
        Task CloseAsync();
        Task<int> CountAsync<T>();
        Task DeleteAsync(object obj);
        Task DeleteAsync(object obj, ITransaction transaction);
        Task<bool> DeleteObjectByAsync(object obj, params string[] fieldNames);
        Task<bool> DeleteObjectByAsync(object obj, ITransaction transaction, params string[] fieldNames);
        Task<bool> DeleteObjectByAsync(string fieldName, object obj);
        Task<int> DeleteObjectByAsync(Type objectType, Dictionary<string, object> criteria);
        Task<int> DeleteObjectByAsync<T>(Dictionary<string, object> criteria);
        Task DropTypeAsync(Type type);
        Task DropTypeAsync(Type type, bool claimFreespace);
        Task DropTypeAsync<T>();
        Task EndBulkInsertAsync(params Type[] types);
        Task FlushAsync();
        Task<List<MetaType>> GetAllTypesAsync();
        Task<IObjectList<T>> LoadAllAsync<T>();
        Task<IObjectList<T>> LoadAllLazyAsync<T>();
        Task<List<int>> LoadAllOIDsAsync(MetaType type);
        Task<T> LoadObjectByOIDAsync<T>(int oid);
        Task<List<int>> LoadOidsAsync<T>(Expression expression);
        Task<object> LoadValueAsync(int oid, string fieldName, MetaType mt);
        Task StartBulkInsertAsync(params Type[] types);
        Task StoreObjectAsync(object obj);
        Task StoreObjectAsync(object obj, ITransaction transaction);
        Task StoreObjectPartiallyAsync(object obj, params string[] properties);
        Task StoreObjectPartiallyAsync(object obj, bool onlyReferences, params string[] properties);
        Task<bool> UpdateObjectByAsync(object obj, params string[] fieldNames);
        Task<bool> UpdateObjectByAsync(object obj, ITransaction transaction, params string[] fieldNames);
        Task<bool> UpdateObjectByAsync(string fieldName, object obj);

#endif
    }
}