using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using sqoDB.Exceptions;
using sqoDB.PropertyResolver;
using sqoDB.Utilities;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB
{
    [Obfuscation(Exclude = true)]
    public abstract class ProjectionRow
    {
        public abstract object GetValue(int index);
    }

    [Obfuscation(Exclude = true)]
    public class ColumnProjection
    {
        internal List<SqoColumn> Columns;
        internal Expression Selector;
    }

    [Obfuscation(Exclude = true)]
    public class ColumnProjector : ExpressionVisitor
    {
        private static MethodInfo miGetValue;
        private List<SqoColumn> columns;
        private int iColumn;
        private ParameterExpression row;

        internal ColumnProjector()
        {
            if (miGetValue == null) miGetValue = typeof(ProjectionRow).GetMethod("GetValue");
        }

        internal ColumnProjection ProjectColumns(Expression expression, ParameterExpression row)
        {
            columns = new List<SqoColumn>();
            this.row = row;
            var selector = Visit(expression);
            return new ColumnProjection { Columns = columns, Selector = selector };
        }

        protected override Expression VisitMemberAccess(MemberExpression m)
        {
            if (m.Expression != null && m.Expression.NodeType == ExpressionType.Parameter)
            {
#if WinRT
                if (m.Member.GetMemberType() == MemberTypes.Property)
#else
                if (m.Member.MemberType == System.Reflection.MemberTypes.Property)
#endif
                {
                    if (m.Member.Name == "OID")
                    {
                        var col = new SqoColumn();
                        col.SourcePropName = m.Member.Name;
                        col.SourceType = m.Expression.Type;
                        columns.Add(col);
                        return Expression.Convert(Expression.Call(row, miGetValue, Expression.Constant(iColumn++)),
                            m.Type);
                    }

                    var pi = m.Member as PropertyInfo;
#if SILVERLIGHT || CF || UNITY3D || WinRT || MONODROID
                        string fieldName = SilverlightPropertyResolver.GetPrivateFieldName(pi, pi.DeclaringType);
                        if (fieldName != null)
                        {
                            SqoColumn col = new SqoColumn();
                            col.SourcePropName = fieldName;
                            col.SourceType = m.Expression.Type;
                            this.columns.Add(col);
                            return Expression.Convert(Expression.Call(this.row, miGetValue, Expression.Constant(iColumn++)), m.Type);

                        }
                        else
                        {
                            string fld = MetaHelper.GetBackingFieldByAttribute(m.Member);
                            if (fld != null)
                            {
                                SqoColumn col = new SqoColumn();
                                col.SourcePropName = fld;
                                col.SourceType = m.Expression.Type;
                                this.columns.Add(col);
                                return Expression.Convert(Expression.Call(this.row, miGetValue, Expression.Constant(iColumn++)), m.Type);

                            }
                            else
                            {
                                throw new SiaqodbException("A Property must have UseVariable Attribute set");
                            }
                        }

#else
                    try
                    {
                        var fi = BackingFieldResolver.GetBackingField(pi);
                        if (fi != null)
                        {
                            var col = new SqoColumn();
                            col.SourcePropName = fi.Name;
                            col.SourceType = m.Expression.Type;
                            columns.Add(col);
                            return Expression.Convert(Expression.Call(row, miGetValue, Expression.Constant(iColumn++)),
                                m.Type);
                        }

                        throw new SiaqodbException("A Property must have UseVariable Attribute set");
                    }
                    catch
                    {
                        var fld = MetaHelper.GetBackingFieldByAttribute(m.Member);
                        if (fld != null)
                        {
                            var col = new SqoColumn();
                            col.SourcePropName = fld;
                            col.SourceType = m.Expression.Type;
                            columns.Add(col);
                            return Expression.Convert(Expression.Call(row, miGetValue, Expression.Constant(iColumn++)),
                                m.Type);
                        }

                        throw new SiaqodbException("A Property must have UseVariable Attribute set");
                    }
#endif
                }
#if WinRT
                else if (m.Member.GetMemberType() == MemberTypes.Field)
#else

                if (m.Member.MemberType == System.Reflection.MemberTypes.Field)
#endif
                {
                    var col = new SqoColumn();
                    col.SourcePropName = m.Member.Name;
                    col.SourceType = m.Expression.Type;
                    columns.Add(col);
                    return Expression.Convert(Expression.Call(row, miGetValue, Expression.Constant(iColumn++)), m.Type);
                }

                throw new NotSupportedException("Not supported Member Type!");
            }

            return base.VisitMemberAccess(m);
        }

        protected override Expression VisitParameter(ParameterExpression p)
        {
            var col = new SqoColumn();
            col.SourcePropName = p.Name;
            col.SourceType = p.Type;
            col.IsFullObject = true;
            columns.Add(col);
            return Expression.Convert(Expression.Call(row, miGetValue, Expression.Constant(iColumn++)), p.Type);
        }
    }

    internal class TranslateResult
    {
        internal List<SqoColumn> Columns;
        internal LambdaExpression Projector;
    }

    internal class QueryTranslatorProjection : ExpressionVisitor
    {
        private ColumnProjection projection;
        private ParameterExpression row;

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote) e = ((UnaryExpression)e).Operand;
            return e;
        }

        internal TranslateResult Translate(Expression m)
        {
            m = Evaluator.PartialEval(m);

            row = Expression.Parameter(typeof(ProjectionRow), "row");
            var lambda = m as LambdaExpression;
            if (lambda == null)
                throw new LINQUnoptimizeException("Expression is type:" + m.NodeType + " and not LambdaExpression");
            var projection = new ColumnProjector().ProjectColumns(lambda.Body, row);

            this.projection = projection;

            return new TranslateResult
            {
                Columns = projection.Columns,
                Projector = this.projection != null ? Expression.Lambda(this.projection.Selector, row) : null
            };
        }
    }

    [Obfuscation(Exclude = true)]
    internal class ProjectionReader<T, TOuter, TInner> : ISqoQuery<T>
    {
        private readonly Enumerator enumerator;
        private readonly Expression innerExpression;

        private readonly Expression outerExpression;
        private readonly ISqoQuery<TInner> SqoQueryInner;
        private readonly ISqoQuery<TOuter> SqoQueryOuter;


        public ProjectionReader(List<SqoColumn> columns, Func<ProjectionRow, T> projector,
            ISqoQuery<TOuter> SqoQueryOuter, ISqoQuery<TInner> SqoQueryInner, Expression outer, Expression inner)
        {
            enumerator = new Enumerator(columns, projector);
            outerExpression = outer;
            innerExpression = inner;
            this.SqoQueryInner = SqoQueryInner;
            this.SqoQueryOuter = SqoQueryOuter;
        }
#if ASYNC
        public async Task<IList<T>> ToListAsync()
        {
            var e = enumerator;
            var SqoQueryOuterImp = SqoQueryOuter as SqoQuery<TOuter>;
            var SqoQueryInnerImp = SqoQueryInner as SqoQuery<TInner>;
            if (SqoQueryOuterImp != null)
            {
                var oids = await SqoQueryOuterImp.Siaqodb.LoadOidsForJoinAsync<T, TOuter, TInner>(SqoQueryOuterImp,
                    SqoQueryInnerImp, outerExpression, innerExpression);

                e.oids = oids;
                e.siaqodb = SqoQueryOuterImp.Siaqodb;
                e.outerType = typeof(TOuter);
                e.innerType = typeof(TInner);
                var list = new List<T>();
                while (e.MoveNext()) list.Add(e.Current);
                return list;
            }

            throw new LINQUnoptimizeException("cannot optimize");
            //this.enumerator = null;
        }
#endif
        public IEnumerator<T> GetEnumerator()
        {
            var e = enumerator;
            var SqoQueryOuterImp = SqoQueryOuter as SqoQuery<TOuter>;
            var SqoQueryInnerImp = SqoQueryInner as SqoQuery<TInner>;
            if (SqoQueryOuterImp != null)
            {
                var oids = SqoQueryOuterImp.Siaqodb.LoadOidsForJoin<T, TOuter, TInner>(SqoQueryOuterImp,
                    SqoQueryInnerImp, outerExpression, innerExpression);

                e.oids = oids;
                e.siaqodb = SqoQueryOuterImp.Siaqodb;
                e.outerType = typeof(TOuter);
                e.innerType = typeof(TInner);
            }
            else
            {
                throw new LINQUnoptimizeException("cannot optimize");
            }

            //this.enumerator = null;
            return e;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private class Enumerator : ProjectionRow, IEnumerator<T>, IEnumerator, IDisposable
        {
            private readonly List<SqoColumn> columns;
            private int currentIndex;
            internal Type innerType;
            internal List<KeyValuePair<int, int>> oids;
            internal Type outerType;
            private readonly Func<ProjectionRow, T> projector;
            internal Siaqodb siaqodb;

            internal Enumerator(List<SqoColumn> columns, Func<ProjectionRow, T> projector)
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

            public override object GetValue(int index)
            {
                var col = columns[index];
                if (col.SourceType == innerType)
                {
                    if (col.IsFullObject)
                        return siaqodb.LoadObjectByOID(col.SourceType, oids[currentIndex].Value);
                    return siaqodb.LoadValue(oids[currentIndex].Value, col.SourcePropName, col.SourceType);
                }

                if (col.IsFullObject)
                    return siaqodb.LoadObjectByOID(col.SourceType, oids[currentIndex].Key);
                return siaqodb.LoadValue(oids[currentIndex].Key, col.SourcePropName, col.SourceType);
            }
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
}