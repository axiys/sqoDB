using System;
using System.Collections.Generic;
using System.Linq;

namespace sqoDB.Cache
{
    public class ConditionalWeakTable
    {
        private readonly Dictionary<object, int> _table;
        private int _capacity = 4;

        public ConditionalWeakTable()
        {
            _table = new Dictionary<object, int>();
        }

        public void Add(object key, int value)
        {
            CleanupDeadReferences();
            _table.Add(CreateWeakKey(key), value);
        }

        public bool Remove(object key)
        {
            return _table.Remove(key);
        }

        public bool TryGetValue(object key, out int value)
        {
            return _table.TryGetValue(key, out value);
        }

        private void CleanupDeadReferences()
        {
            if (_table.Count < _capacity) return;

            var deadKeys = _table.Keys
                .Where(weakRef => !((EquivalentWeakReference)weakRef).IsAlive).ToArray();

            foreach (var deadKey in deadKeys) _table.Remove(deadKey);

            if (_table.Count >= _capacity) _capacity *= 2;
        }

        private static object CreateWeakKey(object key)
        {
            return new EquivalentWeakReference(key);
        }

        private class EquivalentWeakReference
        {
            private readonly int _hashCode;
            private readonly WeakReference _weakReference;

            public EquivalentWeakReference(object obj)
            {
                _hashCode = obj.GetHashCode();
                _weakReference = new WeakReference(obj);
            }

            public bool IsAlive => _weakReference.IsAlive;

            public override bool Equals(object obj)
            {
                var weakRef = obj as EquivalentWeakReference;

                if (weakRef != null) obj = weakRef._weakReference.Target;

                if (obj == null) return base.Equals(weakRef);

                return Equals(_weakReference.Target, obj);
            }

            public override int GetHashCode()
            {
                return _hashCode;
            }
        }
    }
}