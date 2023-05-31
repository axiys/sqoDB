using System.Collections;
using System.Collections.Generic;

namespace sqoDB
{
    /// <summary>
    ///     List used to retrieve objects from database
    /// </summary>
    /// <typeparam name="T">Type of objects from list</typeparam>
    public class ObjectList<T> : IObjectList<T>, IList<T>
    {
        private readonly List<T> list = new List<T>();

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            return list.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region IList<T> Members

        public int IndexOf(T item)
        {
            return list.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            list.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            list.RemoveAt(index);
        }

        public T this[int index]
        {
            get => list[index];
            set => list[index] = value;
        }

        #endregion

        #region ICollection<T> Members

        public void Add(T item)
        {
            list.Add(item);
        }

        public void Clear()
        {
            list.Clear();
        }

        public bool Contains(T item)
        {
            return list.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            list.CopyTo(array, arrayIndex);
        }

        public int Count => list.Count;

        public bool IsReadOnly => false;

        public bool Remove(T item)
        {
            return list.Remove(item);
        }

        #endregion
    }
}