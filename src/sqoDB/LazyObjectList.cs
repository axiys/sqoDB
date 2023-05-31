using System.Collections;
using System.Collections.Generic;
using sqoDB.Exceptions;
using sqoDB.Meta;

namespace sqoDB
{
#if KEVAST
    internal
#else
    public
#endif
        class LazyObjectList<T> : IObjectList<T>
    {
        private readonly List<int> oids;
        private readonly Siaqodb siaqodb;
        private LazyEnumerator<T> enumerator;

        public LazyObjectList(Siaqodb siaqodb, List<int> oids)
        {
            this.oids = oids;
            this.siaqodb = siaqodb;
        }

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            if (enumerator == null) enumerator = new LazyEnumerator<T>(siaqodb, oids);
            return enumerator;
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return enumerator;
        }

        #endregion

        #region IList<T> Members

        public int IndexOf(T item)
        {
            var ti = siaqodb.CheckDBAndGetSqoTypeInfo<T>();
            var objInfo = MetaExtractor.GetObjectInfo(item, ti, siaqodb.metaCache);
            return oids.IndexOf(objInfo.Oid);
        }

        public void Insert(int index, T item)
        {
            throw new SiaqodbException(
                "LazyObjectList does not support this operation because objects are loaded on demand from db");
        }

        public void RemoveAt(int index)
        {
            throw new SiaqodbException(
                "LazyObjectList does not support this operation because objects are loaded on demand from db");
        }

        public T this[int index]
        {
            get
            {
                var obj = siaqodb.LoadObjectByOID<T>(oids[index]);
                return obj;
            }
            set => throw new SiaqodbException(
                "LazyObjectList does not support this operation because objects are loaded on demand from db");
        }

        #endregion

        #region ICollection<T> Members

        public void Add(T item)
        {
            throw new SiaqodbException(
                "LazyObjectList does not support this operation because objects are loaded on demand from db");
        }

        public void Clear()
        {
            throw new SiaqodbException(
                "LazyObjectList does not support this operation because objects are loaded on demand from db");
        }

        public bool Contains(T item)
        {
            var ti = siaqodb.CheckDBAndGetSqoTypeInfo<T>();
            var objInfo = MetaExtractor.GetObjectInfo(item, ti, siaqodb.metaCache);
            return oids.Contains(objInfo.Oid);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            for (var i = 0; i < oids.Count; i++) array[arrayIndex + i] = siaqodb.LoadObjectByOID<T>(oids[i]);
        }

        public int Count => oids.Count;

        public bool IsReadOnly => true;

        public bool Remove(T item)
        {
            throw new SiaqodbException(
                "LazyObjectList does not support this operation because objects are loaded on demand from db");
        }

        #endregion
    }
}