﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
#if ASYNC
using System.Threading.Tasks;
using sqoDB.Exceptions;
#endif

namespace sqoDB
{
    internal class ProjectionSelectReader<T, TSource> : ISqoQuery<T>
    {
        private readonly EnumeratorSelect<T> enumerator;
        private readonly SqoQuery<TSource> query;
        public ProjectionSelectReader(List<SqoColumn> columns, Func<ProjectionRow, T> projector,
            SqoQuery<TSource> query)
        {
            enumerator = new EnumeratorSelect<T>(columns, projector);
            this.query = query;
        }
#if ASYNC
        public async Task<IList<T>> ToListAsync()
        {
            var e = enumerator;
            e.siaqodb = query.Siaqodb;
            var oids = await query.GetFilteredOidsAsync();

            if (oids == null) oids = await e.siaqodb.LoadAllOIDsAsync<TSource>();
            e.oids = oids;

            if (e == null) throw new InvalidOperationException("Cannot enumerate more than once");
            var list = new List<T>();
            while (await e.MoveNextAsync()) list.Add(e.Current);
            return list;
        }
#endif
        public IEnumerator<T> GetEnumerator()
        {
            var e = enumerator;
            e.siaqodb = query.Siaqodb;
            var oids = query.GetFilteredOids();

            if (oids == null) oids = e.siaqodb.LoadAllOIDs<TSource>();
            e.oids = oids;

            if (e == null) throw new InvalidOperationException("Cannot enumerate more than once");

            //this.enumerator = null;
            return e;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }


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

    internal class EnumeratorSelect<T> : ProjectionRow, IEnumerator<T>, IEnumerator, IDisposable
#if ASYNC
        , ISqoAsyncEnumerator<T>
#endif
    {
        private readonly List<SqoColumn> columns;
        private readonly Func<ProjectionRow, T> projector;
#if ASYNC
        private int currentColumnIndex = 0;
#endif
        private int currentIndex;
        internal List<int> oids;
        internal Siaqodb siaqodb;

        internal EnumeratorSelect(List<SqoColumn> columns, Func<ProjectionRow, T> projector)
        {
            this.columns = columns;
            this.projector = projector;
        }

        public T Current { get; private set; }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            //if (this.reader.Read())
            if (oids.Count > currentIndex)
            {
                Current = projector(this);
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

        public void Dispose()
        {
        }
#if ASYNC

        public async Task<bool> MoveNextAsync()
        {
            throw new SiaqodbException("Not supported async operation");
        }
#endif
        public override object GetValue(int index)
        {
            var col = columns[index];
            if (col.IsFullObject)
                return siaqodb.LoadObjectByOID(col.SourceType, oids[currentIndex]);
            return siaqodb.LoadValue(oids[currentIndex], col.SourcePropName, col.SourceType);
        }
#if ASYNC
        public async Task<object> GetValueAsync(int index)
        {
            var col = columns[index];
            if (col.IsFullObject)
                return await siaqodb.LoadObjectByOIDAsync(col.SourceType, oids[currentIndex]);
            return await siaqodb.LoadValueAsync(oids[currentIndex], col.SourcePropName, col.SourceType);
        }
#endif
    }
}