using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using sqoDB.Exceptions;
using sqoDB.Utilities;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB
{
    internal static class SqoQueryExtensionsImpl
    {
        public static ISqoQuery<TSource> Where<TSource>(ISqoQuery<TSource> self,
            Expression<Func<TSource, bool>> expression)
        {
            try
            {
                var qt = new QueryTranslator(true);
                qt.Validate(expression);
                var SqoQuery = self as SqoQuery<TSource>;
                if (SqoQuery == null)
                    throw new LINQUnoptimizeException();
                if (SqoQuery.Expression != null)
                    SqoQuery.Expression = Merge2ExpressionsByAnd(SqoQuery.Expression, expression);
                else
                    SqoQuery.Expression = expression;

                return SqoQuery;
            }
            catch (LINQUnoptimizeException)
            {
                SiaqodbConfigurator.LogMessage(
                    "Expression:" + expression + " cannot be parsed, query runs un-optimized!", VerboseLevel.Warn);
#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Func<TSource, bool> fn = (Func<TSource, bool>)ExpressionCompiler.ExpressionCompiler.Compile(expression);
#else

                var fn = expression.Compile();
#endif

                return new SelectQueryWhere<TSource>(fn, self);
            }
        }

        private static Expression Merge2ExpressionsByAnd<TSource>(Expression expr1,
            Expression<Func<TSource, bool>> expr2)
        {
            var invokedExpr = Expression.Invoke(expr2, ((LambdaExpression)expr1).Parameters);
            return Expression.Lambda<Func<TSource, bool>>
                (Expression.AndAlso(((LambdaExpression)expr1).Body, invokedExpr), ((LambdaExpression)expr1).Parameters);
        }

        public static ISqoQuery<TRet> Select<TSource, TRet>(ISqoQuery<TSource> self,
            Expression<Func<TSource, TRet>> selector)
        {
#if ASYNC
            var fn = selector.Compile();


            return new SelectQuery<TSource, TRet>(fn, self);
#else
            if (typeof(TSource) == typeof(TRet))
            {
				#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Func<TSource, TRet> fn = (Func<TSource, TRet>)ExpressionCompiler.ExpressionCompiler.Compile(selector);
#else

                Func<TSource, TRet> fn = selector.Compile();
#endif

                return new SelectQuery<TSource, TRet>(fn, self);
			}
			try
			{
				SqoQuery<TSource> to = self as SqoQuery<TSource>;
				//SelectQuery<TSource> toSel = self as SelectQuery<TSource>;
				if (to == null )
				{
					throw new Exceptions.LINQUnoptimizeException("MultiJoint not yet supported");
				}
				QueryTranslatorProjection qp = new QueryTranslatorProjection();
				TranslateResult result = qp.Translate(selector);
				#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Delegate projector = ExpressionCompiler.ExpressionCompiler.Compile(result.Projector);
#else

                Delegate projector = result.Projector.Compile();
#endif

                Type elementType = typeof(TRet);


				Type t = typeof(ProjectionSelectReader<,>).MakeGenericType(elementType, typeof(TSource));
				ConstructorInfo ctor =
 t.GetConstructor(new Type[] { typeof(List<SqoColumn>), typeof(Func<ProjectionRow, TRet>), typeof(SqoQuery<TSource>) });
				ProjectionSelectReader<TRet, TSource> r =
 (ProjectionSelectReader<TRet, TSource>)ctor.Invoke(new object[] { result.Columns, projector, (SqoQuery<TSource>)self });


				return r;
			}
			catch (Exceptions.LINQUnoptimizeException ex3)
            {
                SiaqodbConfigurator.LogMessage("Expression:" + selector.ToString() + " cannot be parsed, query runs un-optimized!", VerboseLevel.Warn);
				#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Func<TSource, TRet> fn = (Func<TSource, TRet>)ExpressionCompiler.ExpressionCompiler.Compile(selector);
#else

                Func<TSource, TRet> fn = selector.Compile();
#endif
                return  new SelectQuery<TSource, TRet>(fn, self);
			}
			#if SILVERLIGHT
            catch (MethodAccessException ex)
            {
                throw new SiaqodbException("Siaqodb on Silverlight not support anonymous types, please use a strong Type ");
            }
            #endif
#endif
        }

        public static ISqoQuery<TResult> Join<TOuter, TInner, TKey, TResult>(ISqoQuery<TOuter> outer,
            IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, TInner, TResult>> resultSelector)
        {
            var SqoQueryOuter = outer;
            var SqoQueryInner = (ISqoQuery<TInner>)inner;

            try
            {
                var to = SqoQueryOuter as SqoQuery<TOuter>;
                var toSel = SqoQueryOuter as SelectQuery<TResult, TOuter>;
                if (to == null && toSel == null) throw new LINQUnoptimizeException("MultiJoin not yet supported");
                var tinn = SqoQueryInner as SqoQuery<TInner>;
                var tinnSel = SqoQueryInner as SelectQuery<TResult, TInner>;
                if (tinn == null && tinnSel == null) throw new LINQUnoptimizeException("MultiJoin not yet supported");

                var qp = new QueryTranslatorProjection();

                var result = qp.Translate(resultSelector);
#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Delegate projector = ExpressionCompiler.ExpressionCompiler.Compile(result.Projector);
#else

                var projector = result.Projector.Compile();
#endif

                var elementType = typeof(TResult);

                var t = typeof(ProjectionReader<,,>).MakeGenericType(elementType, typeof(TOuter), typeof(TInner));
                var ctor = t.GetConstructor(new[]
                {
                    typeof(List<SqoColumn>), typeof(Func<ProjectionRow, TResult>), typeof(ISqoQuery<TOuter>),
                    typeof(ISqoQuery<TInner>), typeof(Expression), typeof(Expression)
                });
                var r = (ProjectionReader<TResult, TOuter, TInner>)ctor.Invoke(new object[]
                    { result.Columns, projector, SqoQueryOuter, SqoQueryInner, outerKeySelector, innerKeySelector });


                return r;
            }
            catch (LINQUnoptimizeException ex3)
            {
                SiaqodbConfigurator.LogMessage(
                    "Expression:" + resultSelector + " cannot be parsed, query runs un-optimized!", VerboseLevel.Warn);
#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Func<TOuter, TKey> outerKeySelectorFN =
 (Func<TOuter, TKey>)ExpressionCompiler.ExpressionCompiler.Compile(outerKeySelector);
                Func<TInner, TKey> innerKeySelectorFN =
 (Func<TInner, TKey>)ExpressionCompiler.ExpressionCompiler.Compile(innerKeySelector);
                Func<TOuter, TInner, TResult> resultSelectorFN =
 (Func<TOuter, TInner, TResult>)ExpressionCompiler.ExpressionCompiler.Compile(resultSelector);
#else

                var outerKeySelectorFN = outerKeySelector.Compile();
                var innerKeySelectorFN = innerKeySelector.Compile();
                var resultSelectorFN = resultSelector.Compile();
#endif

                return new SelectQueryJoin<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelectorFN,
                    innerKeySelectorFN, resultSelectorFN);
            }
#if SILVERLIGHT
            catch (MethodAccessException ex)
            {
                throw new SiaqodbException("Siaqodb on Silverlight not support anonymous types, please use a strong Type ");
            }
#endif
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static int Count<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");
            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null) return sqoQ.CountOids();
            var collection = source as ICollection<TSource>;
            if (collection != null) return collection.Count;

            var num = 0;
            foreach (var t in source) num++;
            return num;
        }

#if ASYNC
        public static async Task<int> CountAsync<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");
            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null) return await sqoQ.CountOidsAsync();
            var collection = source as ICollection<TSource>;
            if (collection != null) return collection.Count;

            var num = 0;
            foreach (var t in source) num++;
            return num;
        }
#endif


        public static int Count<TSource>(ISqoQuery<TSource> source, Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null) return sqoQ.CountOids();
            var collection = query as ICollection<TSource>;
            if (collection != null) return collection.Count;

            var num = 0;
            foreach (var t in query) num++;
            return num;
        }
#if ASYNC
        public static async Task<int> CountAsync<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null) return await sqoQ.CountOidsAsync();
            var collection = query as ICollection<TSource>;
            if (collection != null) return collection.Count;

            var num = 0;
            foreach (var t in query) num++;
            return num;
        }
#endif

        public static TSource FirstOrDefault<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            var sqoIncludeQ = source as IncludeSqoQuery<TSource>;

            if (sqoQ != null)
            {
                IEnumerator<TSource> lazyEnum = sqoQ.GetLazyEnumerator();
                if (lazyEnum.MoveNext())
                    return lazyEnum.Current;
                return default;
            }

            if (sqoIncludeQ != null)
            {
                var lazyEnum = sqoIncludeQ.GetEnumerator();
                if (lazyEnum.MoveNext())
                    return lazyEnum.Current;
                return default;
            }

            return ((IEnumerable<TSource>)source).FirstOrDefault();
        }
#if ASYNC
        public static async Task<TSource> FirstOrDefaultAsync<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            var sqoIncludeQ = source as IncludeSqoQuery<TSource>;
            if (sqoQ != null)
            {
                ISqoAsyncEnumerator<TSource> lazyEnum = await sqoQ.GetLazyEnumeratorAsync();
                if (await lazyEnum.MoveNextAsync())
                    return lazyEnum.Current;
                return default;
            }

            if (sqoIncludeQ != null)
            {
                var lazyEnum = await sqoIncludeQ.GetEnumeratorAsync();
                if (await lazyEnum.MoveNextAsync())
                    return lazyEnum.Current;
                return default;
            }

            return ((IEnumerable<TSource>)source).FirstOrDefault();
        }
#endif


        public static TSource FirstOrDefault<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                IEnumerator<TSource> lazyEnum = sqoQ.GetLazyEnumerator();
                if (lazyEnum.MoveNext())
                    return lazyEnum.Current;
                return default;
            }

            using (var enumerator = query.GetEnumerator())
            {
                if (enumerator.MoveNext()) return enumerator.Current;
            }

            return default;
        }
#if ASYNC
        public static async Task<TSource> FirstOrDefaultAsync<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                ISqoAsyncEnumerator<TSource> lazyEnum = await sqoQ.GetLazyEnumeratorAsync();
                if (await lazyEnum.MoveNextAsync())
                    return lazyEnum.Current;
                return default;
            }

            using (var enumerator = query.GetEnumerator())
            {
                if (enumerator.MoveNext()) return enumerator.Current;
            }

            return default;
        }
#endif
        public static TSource First<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            var sqoIncludeQ = source as IncludeSqoQuery<TSource>;
            if (sqoQ != null)
            {
                IEnumerator<TSource> lazyEnum = sqoQ.GetLazyEnumerator();
                if (lazyEnum.MoveNext())
                    return lazyEnum.Current;
                throw new InvalidOperationException("The source sequence is empty.");
            }

            if (sqoIncludeQ != null)
            {
                var lazyEnum = sqoIncludeQ.GetEnumerator();
                if (lazyEnum.MoveNext())
                    return lazyEnum.Current;
                throw new InvalidOperationException("The source sequence is empty.");
            }

            return ((IEnumerable<TSource>)source).First();
        }
#if ASYNC
        public static async Task<TSource> FirstAsync<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            var sqoIncludeQ = source as IncludeSqoQuery<TSource>;
            if (sqoQ != null)
            {
                ISqoAsyncEnumerator<TSource> lazyEnum = await sqoQ.GetLazyEnumeratorAsync();
                if (await lazyEnum.MoveNextAsync())
                    return lazyEnum.Current;
                throw new InvalidOperationException("The source sequence is empty.");
            }

            if (sqoIncludeQ != null)
            {
                var lazyEnum = await sqoIncludeQ.GetEnumeratorAsync();
                if (await lazyEnum.MoveNextAsync())
                    return lazyEnum.Current;
                throw new InvalidOperationException("The source sequence is empty.");
            }

            return ((IEnumerable<TSource>)source).First();
        }
#endif
        public static TSource First<TSource>(ISqoQuery<TSource> source, Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);

            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                IEnumerator<TSource> lazyEnum = sqoQ.GetLazyEnumerator();
                if (lazyEnum.MoveNext())
                    return lazyEnum.Current;
                throw new InvalidOperationException("The source sequence is empty.");
            }

            using (var enumerator = query.GetEnumerator())
            {
                if (enumerator.MoveNext()) return enumerator.Current;
            }

            throw new InvalidOperationException("The source sequence is empty.");
        }
#if ASYNC
        public static async Task<TSource> FirstAsync<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);

            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                ISqoAsyncEnumerator<TSource> lazyEnum = await sqoQ.GetLazyEnumeratorAsync();
                if (await lazyEnum.MoveNextAsync())
                    return lazyEnum.Current;
                throw new InvalidOperationException("The source sequence is empty.");
            }

            using (var enumerator = query.GetEnumerator())
            {
                if (enumerator.MoveNext()) return enumerator.Current;
            }

            throw new InvalidOperationException("The source sequence is empty.");
        }
#endif
        public static bool Any<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");
            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                IEnumerator<TSource> lazyEnum = sqoQ.GetLazyEnumerator();
                if (lazyEnum.MoveNext())
                    return true;
                return false;
            }

            using (var enumerator = source.GetEnumerator())
            {
                if (enumerator.MoveNext()) return true;
            }

            return false;
        }
#if ASYNC
        public static async Task<bool> AnyAsync<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");
            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                ISqoAsyncEnumerator<TSource> lazyEnum = await sqoQ.GetLazyEnumeratorAsync();
                if (await lazyEnum.MoveNextAsync())
                    return true;
                return false;
            }

            using (var enumerator = source.GetEnumerator())
            {
                if (enumerator.MoveNext()) return true;
            }

            return false;
        }
#endif
        public static bool Any<TSource>(ISqoQuery<TSource> source, Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                IEnumerator<TSource> lazyEnum = sqoQ.GetLazyEnumerator();
                if (lazyEnum.MoveNext())
                    return true;
                return false;
            }

            using (var enumerator = query.GetEnumerator())
            {
                if (enumerator.MoveNext()) return true;
            }

            return false;
        }
#if ASYNC
        public static async Task<bool> AnyAsync<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                ISqoAsyncEnumerator<TSource> lazyEnum = await sqoQ.GetLazyEnumeratorAsync();
                if (await lazyEnum.MoveNextAsync())
                    return true;
                return false;
            }

            using (var enumerator = query.GetEnumerator())
            {
                if (enumerator.MoveNext()) return true;
            }

            return false;
        }
#endif
        public static TSource Last<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null) return sqoQ.GetLast(true);
            return ((IEnumerable<TSource>)source).Last();
        }
#if ASYNC
        public static async Task<TSource> LastAsync<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null) return await sqoQ.GetLastAsync(true);
            return ((IEnumerable<TSource>)source).Last();
        }

#endif
        public static TSource Last<TSource>(ISqoQuery<TSource> source, Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null) return sqoQ.GetLast(true);
            return ((IEnumerable<TSource>)query).Last();
        }
#if ASYNC
        public static async Task<TSource> LastAsync<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null) return await sqoQ.GetLastAsync(true);
            return ((IEnumerable<TSource>)query).Last();
        }
#endif
        public static TSource LastOrDefault<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null) return sqoQ.GetLast(false);
            return ((IEnumerable<TSource>)source).LastOrDefault();
        }
#if ASYNC
        public static async Task<TSource> LastOrDefaultAsync<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null) return await sqoQ.GetLastAsync(false);
            return ((IEnumerable<TSource>)source).LastOrDefault();
        }
#endif
        public static TSource LastOrDefault<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null) return sqoQ.GetLast(false);
            return ((IEnumerable<TSource>)query).LastOrDefault();
        }
#if ASYNC
        public static async Task<TSource> LastOrDefaultAsync<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);
            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null) return await sqoQ.GetLastAsync(false);
            return ((IEnumerable<TSource>)query).LastOrDefault();
        }
#endif
        public static TSource Single<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            var includeSqoQ = source as IncludeSqoQuery<TSource>;
            if (sqoQ != null)
            {
                var oids = sqoQ.GetOids();
                if (oids.Count == 1) return sqoQ.Siaqodb.LoadObjectByOID<TSource>(oids[0]);
                if (oids.Count == 0)
                    throw new InvalidOperationException("No match");
                throw new InvalidOperationException("Many matches");
            }

            if (includeSqoQ != null)
            {
                var lazyEnum = includeSqoQ.GetEnumerator();
                var i = 0;
                var obj = default(TSource);
                while (lazyEnum.MoveNext())
                {
                    obj = lazyEnum.Current;
                    i++;
                    if (i > 1)
                        break;
                }

                if (i == 1)
                    return obj;
                if (i == 0)
                    throw new InvalidOperationException("No match");
                throw new InvalidOperationException("Many matches");
            }

            return ((IEnumerable<TSource>)source).Single();
        }
#if ASYNC
        public static async Task<TSource> SingleAsync<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            var includeSqoQ = source as IncludeSqoQuery<TSource>;

            if (sqoQ != null)
            {
                var oids = await sqoQ.GetOidsAsync();
                if (oids.Count == 1) return await sqoQ.Siaqodb.LoadObjectByOIDAsync<TSource>(oids[0]);
                if (oids.Count == 0)
                    throw new InvalidOperationException("No match");
                throw new InvalidOperationException("Many matches");
            }

            if (includeSqoQ != null)
            {
                var lazyEnum = await includeSqoQ.GetEnumeratorAsync();
                var i = 0;
                var obj = default(TSource);
                while (await lazyEnum.MoveNextAsync())
                {
                    obj = lazyEnum.Current;
                    i++;
                    if (i > 1)
                        break;
                }

                if (i == 1)
                    return obj;
                if (i == 0)
                    throw new InvalidOperationException("No match");
                throw new InvalidOperationException("Many matches");
            }

            return ((IEnumerable<TSource>)source).Single();
        }
#endif
        public static TSource Single<TSource>(ISqoQuery<TSource> source, Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);

            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                var oids = sqoQ.GetOids();
                if (oids.Count == 1) return sqoQ.Siaqodb.LoadObjectByOID<TSource>(oids[0]);
                if (oids.Count == 0)
                    throw new InvalidOperationException("No match");
                throw new InvalidOperationException("Many matches");
            }

            return ((IEnumerable<TSource>)query).Single();
        }
#if ASYNC
        public static async Task<TSource> SingleAsync<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);

            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                var oids = await sqoQ.GetOidsAsync();
                if (oids.Count == 1) return await sqoQ.Siaqodb.LoadObjectByOIDAsync<TSource>(oids[0]);
                if (oids.Count == 0)
                    throw new InvalidOperationException("No match");
                throw new InvalidOperationException("Many matches");
            }

            return ((IEnumerable<TSource>)query).Single();
        }
#endif
        public static TSource SingleOrDefault<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            var includeSqoQ = source as IncludeSqoQuery<TSource>;

            if (sqoQ != null)
            {
                var oids = sqoQ.GetOids();
                if (oids.Count == 1) return sqoQ.Siaqodb.LoadObjectByOID<TSource>(oids[0]);
                if (oids.Count == 0)
                    return default;
                throw new InvalidOperationException("Many matches");
            }

            if (includeSqoQ != null)
            {
                var lazyEnum = includeSqoQ.GetEnumerator();
                var i = 0;
                var obj = default(TSource);
                while (lazyEnum.MoveNext())
                {
                    obj = lazyEnum.Current;
                    i++;
                    if (i > 1)
                        break;
                }

                if (i == 1)
                    return obj;
                if (i == 0)
                    return default;
                throw new InvalidOperationException("Many matches");
            }

            return ((IEnumerable<TSource>)source).SingleOrDefault();
        }
#if ASYNC
        public static async Task<TSource> SingleOrDefaultAsync<TSource>(ISqoQuery<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            var includeSqoQ = source as IncludeSqoQuery<TSource>;

            if (sqoQ != null)
            {
                var oids = await sqoQ.GetOidsAsync();
                if (oids.Count == 1) return await sqoQ.Siaqodb.LoadObjectByOIDAsync<TSource>(oids[0]);
                if (oids.Count == 0)
                    return default;
                throw new InvalidOperationException("Many matches");
            }

            if (includeSqoQ != null)
            {
                var lazyEnum = await includeSqoQ.GetEnumeratorAsync();
                var i = 0;
                var obj = default(TSource);
                while (await lazyEnum.MoveNextAsync())
                {
                    obj = lazyEnum.Current;
                    i++;
                    if (i > 1)
                        break;
                }

                if (i == 1)
                    return obj;
                if (i == 0)
                    return default;
                throw new InvalidOperationException("Many matches");
            }

            return ((IEnumerable<TSource>)source).SingleOrDefault();
        }
#endif
        public static TSource SingleOrDefault<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);

            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                var oids = sqoQ.GetOids();
                if (oids.Count == 1) return sqoQ.Siaqodb.LoadObjectByOID<TSource>(oids[0]);
                if (oids.Count == 0)
                    return default;
                throw new InvalidOperationException("Many matches");
            }

            return ((IEnumerable<TSource>)query).SingleOrDefault();
        }
#if ASYNC
        public static async Task<TSource> SingleOrDefaultAsync<TSource>(ISqoQuery<TSource> source,
            Expression<Func<TSource, bool>> expression)
        {
            if (source == null) throw new ArgumentNullException("source");
            var query = Where(source, expression);

            var sqoQ = query as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                var oids = await sqoQ.GetOidsAsync();
                if (oids.Count == 1) return await sqoQ.Siaqodb.LoadObjectByOIDAsync<TSource>(oids[0]);
                if (oids.Count == 0)
                    return default;
                throw new InvalidOperationException("Many matches");
            }

            return ((IEnumerable<TSource>)query).SingleOrDefault();
        }
#endif
        public static ISqoQuery<TSource> Take<TSource>(ISqoQuery<TSource> source, int count)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                if (count <= 0) return source;

                var oids = sqoQ.GetOids();
                if (oids.Count <= count) return source;
                var oidsArr = new int[count];
                oids.CopyTo(0, oidsArr, 0, count);
                return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>(oidsArr));
            }

            var lazyQ = source as LazySqoQuery<TSource>;
            if (lazyQ != null)
            {
                if (count <= 0) return source;

                var oids = lazyQ.GetOids();
                if (oids.Count <= count) return source;
                var oidsArr = new int[count];
                oids.CopyTo(0, oidsArr, 0, count);
                return new LazySqoQuery<TSource>(lazyQ.Siaqodb, new List<int>(oidsArr));
            }

            var orderedQuery = source as SqoOrderedQuery<TSource>;
            if (orderedQuery != null)
            {
                if (count <= 0) return source;

                var oids = orderedQuery.SortAndGetOids();
                if (oids.Count <= count) return source;
                var oidsArr = new int[count];
                oids.CopyTo(0, oidsArr, 0, count);
                return new LazySqoQuery<TSource>(orderedQuery.siaqodb, new List<int>(oidsArr));
            }

            return new SelectQuery<TSource>(((IEnumerable<TSource>)source).Take(count));
        }
#if ASYNC
        public static async Task<ISqoQuery<TSource>> TakeAsync<TSource>(ISqoQuery<TSource> source, int count)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                if (count <= 0) return source;

                var oids = await sqoQ.GetOidsAsync();
                if (oids.Count <= count) return source;
                var oidsArr = new int[count];
                oids.CopyTo(0, oidsArr, 0, count);
                return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>(oidsArr));
            }

            var lazyQ = source as LazySqoQuery<TSource>;
            if (lazyQ != null)
            {
                if (count <= 0) return source;

                var oids = lazyQ.GetOids();
                if (oids.Count <= count) return source;
                var oidsArr = new int[count];
                oids.CopyTo(0, oidsArr, 0, count);
                return new LazySqoQuery<TSource>(lazyQ.Siaqodb, new List<int>(oidsArr));
            }

            return null;
        }
#endif
        public static ISqoQuery<TSource> Skip<TSource>(ISqoQuery<TSource> source, int count)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                if (count <= 0) return source;
                var oids = sqoQ.GetOids();
                if (count >= oids.Count) return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>());

                var oidsArr = new int[oids.Count - count];
                oids.CopyTo(count, oidsArr, 0, oids.Count - count);
                return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>(oidsArr));
            }

            var lazySqo = source as LazySqoQuery<TSource>;
            if (lazySqo != null)
            {
                if (count <= 0) return source;
                var oids = lazySqo.GetOids();
                if (count >= oids.Count) return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>());

                var oidsArr = new int[oids.Count - count];
                oids.CopyTo(count, oidsArr, 0, oids.Count - count);
                return new LazySqoQuery<TSource>(lazySqo.Siaqodb, new List<int>(oidsArr));
            }

            var orderedQuery = source as SqoOrderedQuery<TSource>;
            if (orderedQuery != null)
            {
                if (count <= 0) return source;
                var oids = orderedQuery.SortAndGetOids();
                if (count >= oids.Count) return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>());

                var oidsArr = new int[oids.Count - count];
                oids.CopyTo(count, oidsArr, 0, oids.Count - count);
                return new LazySqoQuery<TSource>(orderedQuery.siaqodb, new List<int>(oidsArr));
            }

            return new SelectQuery<TSource>(((IEnumerable<TSource>)source).Skip(count));
        }
#if ASYNC
        public static async Task<ISqoQuery<TSource>> SkipAsync<TSource>(ISqoQuery<TSource> source, int count)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            if (sqoQ != null)
            {
                if (count <= 0) return source;
                var oids = await sqoQ.GetOidsAsync();
                if (count >= oids.Count) return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>());

                var oidsArr = new int[oids.Count - count];
                oids.CopyTo(count, oidsArr, 0, oids.Count - count);
                return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>(oidsArr));
            }

            var lazySqo = source as LazySqoQuery<TSource>;
            if (lazySqo != null)
            {
                if (count <= 0) return source;
                var oids = lazySqo.GetOids();
                if (count >= oids.Count) return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>());

                var oidsArr = new int[oids.Count - count];
                oids.CopyTo(count, oidsArr, 0, oids.Count - count);
                return new LazySqoQuery<TSource>(lazySqo.Siaqodb, new List<int>(oidsArr));
            }

            var orderedQuery = source as SqoOrderedQuery<TSource>;
            if (orderedQuery != null)
            {
                if (count <= 0) return source;
                var oids = orderedQuery.SortAndGetOids();
                if (count >= oids.Count) return new LazySqoQuery<TSource>(sqoQ.Siaqodb, new List<int>());

                var oidsArr = new int[oids.Count - count];
                oids.CopyTo(count, oidsArr, 0, oids.Count - count);
                return new LazySqoQuery<TSource>(orderedQuery.siaqodb, new List<int>(oidsArr));
            }

            return null;
        }
#endif
        public static ISqoQuery<TSource> Include<TSource>(ISqoQuery<TSource> source, string path)
        {
            if (source == null) throw new ArgumentNullException("source");

            var sqoQ = source as SqoQuery<TSource>;
            var isqoQ = source as IncludeSqoQuery<TSource>;
            if (sqoQ != null) return new IncludeSqoQuery<TSource>(sqoQ, path);

            if (isqoQ != null)
            {
                isqoQ.includes.Add(path);
                return isqoQ;
            }

            throw new SiaqodbException("Include is only allowed on Where or other Include!");
        }
#if !UNITY3D || XIOS

        public static ISqoOrderedQuery<TSource> OrderBy<TSource, TKey>(ISqoQuery<TSource> source,
            Expression<Func<TSource, TKey>> keySelector)
        {
            var select = Select(source, keySelector);
            var r = select as ProjectionSelectReader<TKey, TSource>;
            var sqoQuery = source as SqoQuery<TSource>;
            if (r != null && sqoQuery != null)
            {
                var selectEnum = r.GetEnumerator() as EnumeratorSelect<TKey>;
                var selectOids = selectEnum.oids;
                var orderedList = new List<SqoSortableItem>(selectOids.Count);
                var i = 0;
                foreach (var enumItem in r)
                {
                    var sortableItem = new SqoSortableItem(selectOids[i], enumItem);
                    orderedList.Add(sortableItem);
                    i++;
                }

                var comparer = new SqoComparer<SqoSortableItem>(false);
                var orderedQuery = new SqoOrderedQuery<TSource>(sqoQuery.Siaqodb, orderedList, comparer);


                return orderedQuery;
            }

            SiaqodbConfigurator.LogMessage("Expression:" + keySelector + " cannot be parsed, query runs un-optimized!",
                VerboseLevel.Warn);
#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Func<TSource, TKey> fn =
 (Func<TSource, TKey>)ExpressionCompiler.ExpressionCompiler.Compile(keySelector);
#else

            var fn = keySelector.Compile();
#endif
            return new SqoObjOrderedQuery<TSource>(source.OrderBy(fn));
        }

        public static ISqoOrderedQuery<TSource> OrderByDescending<TSource, TKey>(ISqoQuery<TSource> source,
            Expression<Func<TSource, TKey>> keySelector)
        {
            var select = Select(source, keySelector);
            var r = select as ProjectionSelectReader<TKey, TSource>;
            var sqoQuery = source as SqoQuery<TSource>;
            if (r != null && sqoQuery != null)
            {
                var selectEnum = r.GetEnumerator() as EnumeratorSelect<TKey>;
                var selectOids = selectEnum.oids;
                var orderedList = new List<SqoSortableItem>(selectOids.Count);
                var i = 0;
                foreach (var enumItem in r)
                {
                    var sortableItem = new SqoSortableItem(selectOids[i], enumItem);
                    orderedList.Add(sortableItem);
                    i++;
                }

                var comparer = new SqoComparer<SqoSortableItem>(true);
                var orderedQuery = new SqoOrderedQuery<TSource>(sqoQuery.Siaqodb, orderedList, comparer);


                return orderedQuery;
            }

            SiaqodbConfigurator.LogMessage("Expression:" + keySelector + " cannot be parsed, query runs un-optimized!",
                VerboseLevel.Warn);
#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Func<TSource, TKey> fn =
 (Func<TSource,TKey >)ExpressionCompiler.ExpressionCompiler.Compile(keySelector);
#else

            var fn = keySelector.Compile();
#endif
            return new SqoObjOrderedQuery<TSource>(source.OrderByDescending(fn));
        }

        public static ISqoOrderedQuery<TSource> ThenBy<TSource, TKey>(ISqoOrderedQuery<TSource> source,
            Expression<Func<TSource, TKey>> keySelector)
        {
            var orderedQuery = source as SqoOrderedQuery<TSource>;
            if (orderedQuery != null)
            {
                var qp = new QueryTranslatorProjection();
                var result = qp.Translate(keySelector);
                if (result.Columns.Count == 1)
                {
                    orderedQuery.comparer.AddOrder(false);
                    foreach (var item in orderedQuery.SortableItems)
                        item.Add(orderedQuery.siaqodb.LoadValue(item.oid, result.Columns[0].SourcePropName,
                            typeof(TSource)));
                    return orderedQuery;
                }
            }

            SiaqodbConfigurator.LogMessage("Expression:" + keySelector + " cannot be parsed, query runs un-optimized!",
                VerboseLevel.Warn);
#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Func<TSource, TKey> fn =
 (Func<TSource, TKey>)ExpressionCompiler.ExpressionCompiler.Compile(keySelector);
#else

            var fn = keySelector.Compile();
#endif

            return new SqoObjOrderedQuery<TSource>(source.ThenBy(fn));
        }

        public static ISqoOrderedQuery<TSource> ThenByDescending<TSource, TKey>(ISqoOrderedQuery<TSource> source,
            Expression<Func<TSource, TKey>> keySelector)
        {
            var orderedQuery = source as SqoOrderedQuery<TSource>;
            if (orderedQuery != null)
            {
                var qp = new QueryTranslatorProjection();
                var result = qp.Translate(keySelector);
                if (result.Columns.Count == 1)
                {
                    orderedQuery.comparer.AddOrder(true);
                    foreach (var item in orderedQuery.SortableItems)
                        item.Add(orderedQuery.siaqodb.LoadValue(item.oid, result.Columns[0].SourcePropName,
                            typeof(TSource)));
                    return orderedQuery;
                }
            }

            SiaqodbConfigurator.LogMessage("Expression:" + keySelector + " cannot be parsed, query runs un-optimized!",
                VerboseLevel.Warn);
#if (WP7 || UNITY3D) && !MANGO && !XIOS
                Func<TSource, TKey> fn =
 (Func<TSource, TKey>)ExpressionCompiler.ExpressionCompiler.Compile(keySelector);
#else

            var fn = keySelector.Compile();
#endif
            return new SqoObjOrderedQuery<TSource>(source.ThenByDescending(fn));
        }
#endif
    }
}