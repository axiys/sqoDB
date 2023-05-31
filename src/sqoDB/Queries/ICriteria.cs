using System.Collections.Generic;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Queries
{
    internal interface ICriteria
    {
        List<int> GetOIDs();
#if ASYNC
        Task<List<int>> GetOIDsAsync();
#endif
    }
}