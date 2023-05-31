using System.Collections.Generic;
using sqoDB.Core;
using sqoDB.Meta;

namespace sqoDB.Transactions
{
    internal class TransactionInternal
    {
        internal Dictionary<ObjectSerializer, KeyValuePair<SqoTypeInfo, int>> nrRecordsBeforeCommit =
            new Dictionary<ObjectSerializer, KeyValuePair<SqoTypeInfo, int>>();

        internal Siaqodb siaqodbInstance;
        internal List<SqoTypeInfo> tiInvolvedInTransaction = new List<SqoTypeInfo>();
        internal Transaction transaction;
        internal List<TransactionObject> transactionObjects = new List<TransactionObject>();

        public TransactionInternal(Transaction tr, Siaqodb siaqodb)
        {
            transaction = tr;
            siaqodbInstance = siaqodb;
        }

        public void AddTransactionObject(TransactionObject trObj)
        {
            transactionObjects.Add(trObj);
            if (!tiInvolvedInTransaction.Contains(trObj.objInfo.SqoTypeInfo))
                tiInvolvedInTransaction.Add(trObj.objInfo.SqoTypeInfo);
        }
    }
}