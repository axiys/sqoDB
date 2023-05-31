using System.Collections;
using System.Collections.Generic;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB
{
#if KEVAST
    internal
#else
    public
#endif
        class LazyEnumerator<T> : IEnumerator<T>, IEnumerator
#if ASYNC
            , ISqoAsyncEnumerator<T>
#endif
    {
        private readonly List<int> oids;
        private readonly List<string> propertiesIncluded;

        private readonly Siaqodb siaqodb;
        private int currentIndex;

        public LazyEnumerator(Siaqodb siaqodb, List<int> oids)
        {
            this.siaqodb = siaqodb;
            this.oids = oids;
        }

        public LazyEnumerator(Siaqodb siaqodb, List<int> oids, List<string> includes)
        {
            this.siaqodb = siaqodb;
            this.oids = oids;
            propertiesIncluded = includes;
        }

        #region IEnumerator<T> Members

        public T Current { get; private set; }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion

#if ASYNC
        public async Task<bool> MoveNextAsync()
        {
            if (oids.Count > currentIndex)
            {
                if (propertiesIncluded == null)
                    Current = await siaqodb.LoadObjectByOIDAsync<T>(oids[currentIndex]);
                else
                    Current = await siaqodb.LoadObjectByOIDAsync<T>(oids[currentIndex], propertiesIncluded);
                currentIndex++;
                return true;
            }

            Reset();
            return false;
        }
#endif

        #region IEnumerator Members

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (oids.Count > currentIndex)
            {
                if (propertiesIncluded == null)
                    Current = siaqodb.LoadObjectByOID<T>(oids[currentIndex]);
                else
                    Current = siaqodb.LoadObjectByOID<T>(oids[currentIndex], propertiesIncluded);
                currentIndex++;
                return true;
            }

            Reset();
            return false;
        }

        public void Reset()
        {
            currentIndex = 0;
        }

        #endregion
    }
#if ASYNC
    public interface ISqoAsyncEnumerator<T> : IEnumerator<T>
    {
        T Current { get; }

        Task<bool> MoveNextAsync();
        void Reset();
    }
#endif
}