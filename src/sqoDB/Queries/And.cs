using System.Collections.Generic;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Queries
{
    internal class And : ICriteria
    {
        private ICriteria criteria1;
        private ICriteria criteria2;

        public void Add(ICriteria criteria1, ICriteria criteria2)
        {
            this.criteria1 = criteria1;
            this.criteria2 = criteria2;
        }


        #region ICriteria Members

        public List<int> GetOIDs()
        {
            var list = new List<int>();
            var unu = criteria1.GetOIDs();
            var doi = criteria2.GetOIDs();

            if (unu.Count < doi.Count)
            {
                doi.Sort();

                foreach (var oid in unu)
                {
                    var index = doi.BinarySearch(oid);
                    if (index >= 0) list.Add(doi[index]);
                }
            }
            else
            {
                unu.Sort();
                foreach (var oid in doi)
                {
                    var index = unu.BinarySearch(oid);
                    if (index >= 0) list.Add(unu[index]);
                }
            }

            return list;
        }


#if ASYNC
        public async Task<List<int>> GetOIDsAsync()
        {
            var list = new List<int>();
            var unu = await criteria1.GetOIDsAsync().ConfigureAwait(false);
            var doi = await criteria2.GetOIDsAsync().ConfigureAwait(false);

            if (unu.Count < doi.Count)
            {
                doi.Sort();

                foreach (var oid in unu)
                {
                    var index = doi.BinarySearch(oid);
                    if (index >= 0) list.Add(doi[index]);
                }
            }
            else
            {
                unu.Sort();
                foreach (var oid in doi)
                {
                    var index = unu.BinarySearch(oid);
                    if (index >= 0) list.Add(unu[index]);
                }
            }

            return list;
        }

#endif

        #endregion
    }
}