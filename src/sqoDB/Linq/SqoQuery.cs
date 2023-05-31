using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
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
        class SqoQuery<T> : ISqoQuery<T>
    {
        private List<int> oidsList;
        private IObjectList<T> oList;

        public SqoQuery(Siaqodb siaqodb)
        {
            Siaqodb = siaqodb;
        }

        public Expression Expression { get; set; }

        public Siaqodb Siaqodb { get; }

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            if (oList == null)
            {
                if (Expression == null)
                    oList = Siaqodb.LoadAll<T>();
                else
                    oList = Siaqodb.Load<T>(Expression);
            }

            return oList.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        public List<int> GetFilteredOids()
        {
            if (Expression == null)
                return null;
            return Siaqodb.LoadOids<T>(Expression);
        }
#if ASYNC
        public async Task<List<int>> GetFilteredOidsAsync()
        {
            if (Expression == null)
                return null;
            return await Siaqodb.LoadOidsAsync<T>(Expression);
        }
#endif
        public int CountOids()
        {
            if (Expression == null)
                return Siaqodb.Count<T>();
            return Siaqodb.LoadOids<T>(Expression).Count;
        }

        public LazyEnumerator<T> GetLazyEnumerator()
        {
            if (oidsList == null)
            {
                if (Expression == null)
                    oidsList = Siaqodb.LoadAllOIDs<T>();
                else
                    oidsList = Siaqodb.LoadOids<T>(Expression);
            }

            return new LazyEnumerator<T>(Siaqodb, oidsList);
        }
#if ASYNC
        public async Task<LazyEnumerator<T>> GetLazyEnumeratorAsync()
        {
            if (oidsList == null)
            {
                if (Expression == null)
                    oidsList = await Siaqodb.LoadAllOIDsAsync<T>();
                else
                    oidsList = await Siaqodb.LoadOidsAsync<T>(Expression);
            }

            return new LazyEnumerator<T>(Siaqodb, oidsList);
        }
#endif
        public T GetLast(bool throwExce)
        {
            if (oidsList == null)
            {
                if (Expression == null)
                    oidsList = Siaqodb.LoadAllOIDs<T>();
                else
                    oidsList = Siaqodb.LoadOids<T>(Expression);
            }

            if (oidsList.Count > 0) return Siaqodb.LoadObjectByOID<T>(oidsList[oidsList.Count - 1]);

            if (throwExce)
                throw new InvalidOperationException("no match found");
            return default;
        }
#if ASYNC
        public async Task<T> GetLastAsync(bool throwExce)
        {
            if (oidsList == null)
            {
                if (Expression == null)
                    oidsList = await Siaqodb.LoadAllOIDsAsync<T>();
                else
                    oidsList = await Siaqodb.LoadOidsAsync<T>(Expression);
            }

            if (oidsList.Count > 0) return await Siaqodb.LoadObjectByOIDAsync<T>(oidsList[oidsList.Count - 1]);

            if (throwExce)
                throw new InvalidOperationException("no match found");
            return default;
        }
#endif
        public List<int> GetOids()
        {
            if (Expression == null)
                return Siaqodb.LoadAllOIDs<T>();
            return Siaqodb.LoadOids<T>(Expression);
        }
#if ASYNC
        public async Task<List<int>> GetOidsAsync()
        {
            if (Expression == null)
                return await Siaqodb.LoadAllOIDsAsync<T>();
            return await Siaqodb.LoadOidsAsync<T>(Expression);
        }
#endif
#if ASYNC
        public async Task<int> CountOidsAsync()
        {
            if (Expression == null) return await Siaqodb.CountAsync<T>();

            var list = await Siaqodb.LoadOidsAsync<T>(Expression);
            return list.Count;
        }

        public async Task<IList<T>> ToListAsync()
        {
            if (oList == null)
            {
                if (Expression == null)
                    oList = await Siaqodb.LoadAllAsync<T>();
                else
                    oList = await Siaqodb.LoadAsync<T>(Expression);
            }

            return oList;
        }
#endif


        #region ISqoQuery<T> Members

        public ISqoQuery<T> SqoWhere(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.Where(this, expression);
        }

        public ISqoQuery<TRet> SqoSelect<TRet>(Expression<Func<T, TRet>> selector)
        {
            return SqoQueryExtensionsImpl.Select(this, selector);
        }

        public ISqoQuery<TResult> SqoJoin<TInner, TKey, TResult>(IEnumerable<TInner> inner,
            Expression<Func<T, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector,
            Expression<Func<T, TInner, TResult>> resultSelector)
        {
            return SqoQueryExtensionsImpl.Join(this, inner, outerKeySelector, innerKeySelector, resultSelector);
        }

        public int SqoCount()
        {
            return SqoQueryExtensionsImpl.Count(this);
        }
#if ASYNC
        public Task<int> SqoCountAsync()
        {
            return SqoQueryExtensionsImpl.CountAsync(this);
        }
#endif
        public int SqoCount(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.Count(this, expression);
        }
#if ASYNC
        public Task<int> SqoCountAsync(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.CountAsync(this, expression);
        }
#endif
        public T SqoFirstOrDefault()
        {
            return SqoQueryExtensionsImpl.FirstOrDefault(this);
        }
#if ASYNC
        public Task<T> SqoFirstOrDefaultAsync()
        {
            return SqoQueryExtensionsImpl.FirstOrDefaultAsync(this);
        }
#endif
        public T SqoFirstOrDefault(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.FirstOrDefault(this, expression);
        }
#if ASYNC
        public Task<T> SqoFirstOrDefaultAsync(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.FirstOrDefaultAsync(this, expression);
        }
#endif
        public T SqoFirst()
        {
            return SqoQueryExtensionsImpl.First(this);
        }
#if ASYNC
        public Task<T> SqoFirstAsync()
        {
            return SqoQueryExtensionsImpl.FirstAsync(this);
        }
#endif
        public T SqoFirst(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.First(this, expression);
        }
#if ASYNC
        public Task<T> SqoFirstAsync(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.FirstAsync(this, expression);
        }
#endif
        public bool SqoAny()
        {
            return SqoQueryExtensionsImpl.Any(this);
        }
#if ASYNC
        public Task<bool> SqoAnyAsync()
        {
            return SqoQueryExtensionsImpl.AnyAsync(this);
        }
#endif
        public bool SqoAny(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.Any(this, expression);
        }
#if ASYNC
        public Task<bool> SqoAnyAsync(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.AnyAsync(this, expression);
        }
#endif
        public T SqoLast()
        {
            return SqoQueryExtensionsImpl.Last(this);
        }
#if ASYNC
        public Task<T> SqoLastAsync()
        {
            return SqoQueryExtensionsImpl.LastAsync(this);
        }
#endif
        public T SqoLast(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.Last(this, expression);
        }
#if ASYNC
        public Task<T> SqoLastAsync(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.LastAsync(this, expression);
        }
#endif
        public T SqoLastOrDefault()
        {
            return SqoQueryExtensionsImpl.LastOrDefault(this);
        }
#if ASYNC
        public Task<T> SqoLastOrDefaultAsync()
        {
            return SqoQueryExtensionsImpl.LastOrDefaultAsync(this);
        }
#endif
        public T SqoLastOrDefault(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.LastOrDefault(this, expression);
        }
#if ASYNC
        public Task<T> SqoLastOrDefaultAsync(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.LastOrDefaultAsync(this, expression);
        }
#endif
        public T SqoSingle()
        {
            return SqoQueryExtensionsImpl.Single(this);
        }
#if ASYNC
        public Task<T> SqoSingleAsync()
        {
            return SqoQueryExtensionsImpl.SingleAsync(this);
        }
#endif
        public T SqoSingle(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.Single(this, expression);
        }
#if ASYNC
        public Task<T> SqoSingleAsync(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.SingleAsync(this, expression);
        }
#endif
        public T SqoSingleOrDefault()
        {
            return SqoQueryExtensionsImpl.SingleOrDefault(this);
        }
#if ASYNC
        public Task<T> SqoSingleOrDefaultAsync()
        {
            return SqoQueryExtensionsImpl.SingleOrDefaultAsync(this);
        }
#endif
        public T SqoSingleOrDefault(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.SingleOrDefault(this, expression);
        }
#if ASYNC
        public Task<T> SqoSingleOrDefaultAsync(Expression<Func<T, bool>> expression)
        {
            return SqoQueryExtensionsImpl.SingleOrDefaultAsync(this, expression);
        }
#endif
        public ISqoQuery<T> SqoTake(int count)
        {
            return SqoQueryExtensionsImpl.Take(this, count);
        }
#if ASYNC
        public Task<ISqoQuery<T>> SqoTakeAsync(int count)
        {
            return SqoQueryExtensionsImpl.TakeAsync(this, count);
        }
#endif
        public ISqoQuery<T> SqoSkip(int count)
        {
            return SqoQueryExtensionsImpl.Skip(this, count);
        }
#if ASYNC
        public Task<ISqoQuery<T>> SqoSkipAsync(int count)
        {
            return SqoQueryExtensionsImpl.SkipAsync(this, count);
        }
#endif
        public ISqoQuery<T> SqoInclude(string path)
        {
            return SqoQueryExtensionsImpl.Include(this, path);
        }

#if !UNITY3D || XIOS
        public ISqoOrderedQuery<T> SqoOrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            return SqoQueryExtensionsImpl.OrderBy(this, keySelector);
        }

        public ISqoOrderedQuery<T> SqoOrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            return SqoQueryExtensionsImpl.OrderByDescending(this, keySelector);
        }

        public ISqoOrderedQuery<T> SqoThenBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            return SqoQueryExtensionsImpl.ThenBy(this as ISqoOrderedQuery<T>, keySelector);
        }

        public ISqoOrderedQuery<T> SqoThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            return SqoQueryExtensionsImpl.ThenByDescending(this as ISqoOrderedQuery<T>, keySelector);
        }
#endif

        #endregion
    }
}