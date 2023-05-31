using System.Collections.Generic;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Transactions
{
    public interface ITransaction
    {
        void Commit();
        IList<T> GetUnCommittedObjects<T>();
        IList<T> GetUnCommittedObjects<T>(bool includeDeletes);
        void Rollback();
#if ASYNC
        Task CommitAsync();
        Task RollbackAsync();
#endif
    }
}