using System.Collections.Generic;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Queries
{
    internal class Or : ICriteria
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

            #region old version

            /*foreach (int oid in unu)
            {

                list.Add(oid);

            }
            foreach (int oid in doi)
            {
                if (!list.Contains(oid))
                {
                    list.Add(oid);
                }

            }*/

            #endregion

            if (unu.Count < doi.Count)
            {
                foreach (var oid in doi) list.Add(oid);
                doi.Sort();
                foreach (var oid in unu)
                {
                    var index = doi.BinarySearch(oid);
                    if (index < 0) list.Add(oid);
                }

                return list;
            }

            foreach (var oid in unu) list.Add(oid);
            unu.Sort();
            foreach (var oid in doi)
            {
                var index = unu.BinarySearch(oid);
                if (index < 0) list.Add(oid);
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
                foreach (var oid in doi) list.Add(oid);
                doi.Sort();
                foreach (var oid in unu)
                {
                    var index = doi.BinarySearch(oid);
                    if (index < 0) list.Add(oid);
                }

                return list;
            }

            foreach (var oid in unu) list.Add(oid);
            unu.Sort();
            foreach (var oid in doi)
            {
                var index = unu.BinarySearch(oid);
                if (index < 0) list.Add(oid);
            }

            return list;
        }
#endif

        #endregion
    }
}