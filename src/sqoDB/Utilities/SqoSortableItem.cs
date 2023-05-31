using System;
using System.Collections.Generic;

namespace sqoDB.Utilities
{
    internal class SqoSortableItem
    {
        public List<object> items = new List<object>();
        public int oid;

        public SqoSortableItem(int oid, object value)
        {
            this.oid = oid;
            items.Add(value);
        }

        public void Add(object value)
        {
            items.Add(value);
        }
    }

    internal class SortClass
    {
        public bool desc;
        public int index;

        public SortClass(int index, bool desc)
        {
            this.index = index;
            this.desc = desc;
        }
    }

    internal class SqoComparer<T> : IComparer<T> where T : SqoSortableItem
    {
        private readonly List<bool> sortOrder = new List<bool>();

        public SqoComparer(bool desc)
        {
            sortOrder.Add(desc);
        }

        #region IComparer<T> Members

        public int Compare(T x, T y)
        {
            if (sortOrder.Count == 0) return 0;
            return CheckSort(x, y);
        }

        #endregion

        public void AddOrder(bool desc)
        {
            sortOrder.Add(desc);
        }

        private int CheckSort(SqoSortableItem MyObject1, SqoSortableItem MyObject2)
        {
            var returnVal = 0;

            for (var i = 0; i < MyObject1.items.Count; i++)
            {
                var valueOf1 = MyObject1.items[i];
                var valueOf2 = MyObject2.items[i];
                var result = ((IComparable)valueOf1).CompareTo((IComparable)valueOf2);
                if (result != 0)
                {
                    if (sortOrder[i]) //if desc
                        return -result;
                    return result;
                }
            }

            return returnVal;
        }
    }
}