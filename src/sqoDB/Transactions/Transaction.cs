using System;
using System.Collections.Generic;
using System.Linq;
using sqoDB.Exceptions;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Transactions
{
    public class Transaction : ITransaction
    {
        internal Guid ID;
        internal TransactionStatus status;

        internal Transaction()
        {
            ID = Guid.NewGuid();
        }

        /// <summary>
        ///     Commit transaction to database
        /// </summary>
        public void Commit()
        {
            if (status == TransactionStatus.Closed) throw new SiaqodbException("Transaction closed");
            TransactionManager.CommitTransaction(ID);
        }
#if ASYNC
        /// <summary>
        ///     Commit transaction to database
        /// </summary>
        public async Task CommitAsync()
        {
            if (status == TransactionStatus.Closed) throw new SiaqodbException("Transaction closed");
            await TransactionManager.CommitTransactionAsync(ID);
        }
#endif

        /// <summary>
        ///     Rollback changes
        /// </summary>
        public void Rollback()
        {
            if (status == TransactionStatus.Closed) throw new SiaqodbException("Transaction closed");
            TransactionManager.RollbackTransaction(ID);
        }
#if ASYNC
        /// <summary>
        ///     Rollback changes
        /// </summary>
        public async Task RollbackAsync()
        {
            if (status == TransactionStatus.Closed) throw new SiaqodbException("Transaction closed");
            await TransactionManager.RollbackTransactionAsync(ID);
        }
#endif
        /// <summary>
        ///     Load un-committen objects,except deleted instances
        /// </summary>
        /// <typeparam name="T">Type of un-committen objects</typeparam>
        /// <returns></returns>
        public IList<T> GetUnCommittedObjects<T>()
        {
            if (status == TransactionStatus.Closed) throw new SiaqodbException("Transaction closed");
            IList<TransactionObject> instances = TransactionManager.transactions[ID].transactionObjects.Where(
                trObj => trObj.Operation == TransactionObject.OperationType.InsertOrUpdate &&
                         trObj.currentObject.GetType() == typeof(T)).ToList();
            var uncommittedInstances = new List<T>();
            foreach (var trObj in instances) uncommittedInstances.Add((T)trObj.currentObject);
            return uncommittedInstances;
        }

        /// <summary>
        ///     Load un-committen objects
        /// </summary>
        /// <typeparam name="T">Type of un-committen objects</typeparam>
        /// <param name="includeDeletes">If true, will be returned also deleted objects within the transaction</param>
        /// <returns></returns>
        public IList<T> GetUnCommittedObjects<T>(bool includeDeletes)
        {
            if (status == TransactionStatus.Closed) throw new SiaqodbException("Transaction closed");
            IList<TransactionObject> instances = null;
            if (includeDeletes)
                instances = TransactionManager.transactions[ID].transactionObjects.Where(
                    trObj => trObj.currentObject.GetType() == typeof(T)).ToList();
            else
                instances = TransactionManager.transactions[ID].transactionObjects.Where(
                    trObj => trObj.Operation == TransactionObject.OperationType.InsertOrUpdate &&
                             trObj.currentObject.GetType() == typeof(T)).ToList();
            var uncommittedInstances = new List<T>();
            foreach (var trObj in instances) uncommittedInstances.Add((T)trObj.currentObject);
            return uncommittedInstances;
        }
    }

    internal enum TransactionStatus
    {
        Open,
        Closed
    }
}