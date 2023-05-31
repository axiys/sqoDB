

using System;
using System.Collections.Generic;
using System.Text;
using System.Linq.Expressions;
using sqoDB.Queries;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Queries
{
	
	internal class And : ICriteria
	{
        public And()
        {

        }
        ICriteria criteria1;
        ICriteria criteria2;
        public void Add(ICriteria criteria1, ICriteria criteria2)
        {
            this.criteria1 = criteria1;
            this.criteria2 = criteria2;
        }



        #region ICriteria Members

        public List<int> GetOIDs()
        {
            List<int> list = new List<int>();
            List<int> unu =criteria1.GetOIDs();
            List<int> doi =criteria2.GetOIDs();

			if (unu.Count < doi.Count)
			{
				doi.Sort();

				foreach (int oid in unu)
				{
					int index = doi.BinarySearch(oid);
					if (index >= 0)
					{
						list.Add(doi[index]);
					}
				}
			}
			else
			{
				unu.Sort();
				foreach (int oid in doi)
				{
					int index = unu.BinarySearch(oid);
					if (index >= 0)
					{
						list.Add(unu[index]);
					}
				}
				
			}
            return list;
        }

        
#if ASYNC
        public async Task<List<int>> GetOIDsAsync()
        {
            List<int> list = new List<int>();
            List<int> unu = await criteria1.GetOIDsAsync().ConfigureAwait(false);
            List<int> doi = await criteria2.GetOIDsAsync().ConfigureAwait(false);

            if (unu.Count < doi.Count)
            {
                doi.Sort();

                foreach (int oid in unu)
                {
                    int index = doi.BinarySearch(oid);
                    if (index >= 0)
                    {
                        list.Add(doi[index]);
                    }
                }
            }
            else
            {
                unu.Sort();
                foreach (int oid in doi)
                {
                    int index = unu.BinarySearch(oid);
                    if (index >= 0)
                    {
                        list.Add(unu[index]);
                    }
                }

            }
            return list;
        }

#endif
#endregion
    }
	
}
