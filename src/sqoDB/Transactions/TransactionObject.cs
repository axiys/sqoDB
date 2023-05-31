using System.Reflection;
using sqoDB.Attributes;
using sqoDB.Core;
using sqoDB.Meta;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Transactions
{
    internal class TransactionObject
    {
        public enum OperationType
        {
            InsertOrUpdate,
            Delete
        }

        public object currentObject;
        public StorageEngine engine;
        public ObjectInfo objInfo;
        public OperationType Operation;
        public object originalObject; //for rollback
        public ObjectSerializer serializer;


        public TransactionObject(StorageEngine storageEngine)
        {
            engine = storageEngine;
        }

        public TransactionObjectHeader PreCommit()
        {
            if (objInfo.Oid > 0 && objInfo.Oid <= objInfo.SqoTypeInfo.Header.numberOfRecords &&
                !serializer.IsObjectDeleted(objInfo.Oid, objInfo.SqoTypeInfo))
            {
                originalObject = engine.LoadObjectByOID(objInfo.SqoTypeInfo, objInfo.Oid);
                var header = new TransactionObjectHeader();
                header.Operation = Operation;
                header.OIDofObject = engine.metaCache.GetOIDOfObject(originalObject, objInfo.SqoTypeInfo);
                return header;
            }

            return null;
        }
#if ASYNC
        public async Task<TransactionObjectHeader> PreCommitAsync()
        {
            if (objInfo.Oid > 0 && objInfo.Oid <= objInfo.SqoTypeInfo.Header.numberOfRecords &&
                !await serializer.IsObjectDeletedAsync(objInfo.Oid, objInfo.SqoTypeInfo).ConfigureAwait(false))
            {
                originalObject = await engine.LoadObjectByOIDAsync(objInfo.SqoTypeInfo, objInfo.Oid)
                    .ConfigureAwait(false);
                var header = new TransactionObjectHeader();
                header.Operation = Operation;
                header.OIDofObject = engine.metaCache.GetOIDOfObject(originalObject, objInfo.SqoTypeInfo);
                return header;
            }

            return null;
        }
#endif
        public void Commit()
        {
            if (Operation == OperationType.InsertOrUpdate)
                engine.SaveObject(currentObject, objInfo.SqoTypeInfo, objInfo);
            else
                engine.DeleteObject(currentObject, objInfo.SqoTypeInfo);
        }
#if ASYNC
        public async Task CommitAsync()
        {
            if (Operation == OperationType.InsertOrUpdate)
                await engine.SaveObjectAsync(currentObject, objInfo.SqoTypeInfo, objInfo).ConfigureAwait(false);
            else
                await engine.DeleteObjectAsync(currentObject, objInfo.SqoTypeInfo).ConfigureAwait(false);
        }
#endif
        public void Rollback()
        {
            if (originalObject != null)
            {
                if (Operation == OperationType.InsertOrUpdate)
                    engine.RollbackObject(originalObject, objInfo.SqoTypeInfo);
                else //delete
                    engine.RollbackDeletedObject(originalObject, objInfo.SqoTypeInfo);
            }
        }
#if ASYNC
        public async Task RollbackAsync()
        {
            if (originalObject != null)
            {
                if (Operation == OperationType.InsertOrUpdate)
                    await engine.RollbackObjectAsync(originalObject, objInfo.SqoTypeInfo).ConfigureAwait(false);
                else //delete
                    await engine.RollbackDeletedObjectAsync(originalObject, objInfo.SqoTypeInfo).ConfigureAwait(false);
            }
        }
#endif
    }

    [Obfuscation(Exclude = true)]
    internal class TransactionObjectHeader
    {
        public int OID { get; set; }
        public long Position;
        public int BatchSize;
        public int OIDofObject;
        [MaxLength(500)] public string TypeName;
        public TransactionObject.OperationType Operation;

#if SILVERLIGHT
        public object GetValue(System.Reflection.FieldInfo field)
        {
            return field.GetValue(this);
        }
        public void SetValue(System.Reflection.FieldInfo field, object value)
        {

            field.SetValue(this, value);

        }
#endif
    }

    [Obfuscation(Exclude = true)]
    internal class TransactionTypeHeader
    {
        public int OID { get; set; }
        [MaxLength(500)] public string TypeName;
        public int NumberOfRecords;
#if SILVERLIGHT
        public object GetValue(System.Reflection.FieldInfo field)
        {
            return field.GetValue(this);
        }
        public void SetValue(System.Reflection.FieldInfo field, object value)
        {

            field.SetValue(this, value);

        }
#endif
    }
}