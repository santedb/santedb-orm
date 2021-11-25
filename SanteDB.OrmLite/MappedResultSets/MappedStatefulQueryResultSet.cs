using SanteDB.Core.Model.Query;
using SanteDB.OrmLite;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using SanteDB.Core.Model;
using SanteDB.Core.Services;
using SanteDB.Core.i18n;

namespace SanteDB.OrmLite.MappedResultSets
{
    /// <summary>
    /// Represents a stateful result set
    /// </summary>
    public class MappedStatefulQueryResultSet<TData> : IQueryResultSet<TData>, IDisposable
    {
        // Last fetched UUIDs
        private IEnumerable<Guid> m_lastFetched;

        // The data context
        private readonly DataContext m_context;

        // The query provider
        private readonly IMappedQueryProvider<TData> m_provider;

        // The query identifier
        private Guid m_queryId;

        // Laste count
        private int m_count;

        // Last offset
        private int m_offset;

        /// <summary>
        /// Creates a new persistence collection
        /// </summary>
        internal MappedStatefulQueryResultSet(IMappedQueryProvider<TData> dataProvider, Guid queryId, int count)
        {
            this.m_provider = dataProvider;
            this.m_context = dataProvider.Provider.GetReadonlyConnection();
            this.m_queryId = queryId;
            this.m_count = count;
        }

        /// <summary>
        /// Create a wrapper persistence collection
        /// </summary>
        private MappedStatefulQueryResultSet(MappedStatefulQueryResultSet<TData> copyFrom) : this(copyFrom.m_provider, copyFrom.m_queryId, copyFrom.m_count)
        {
            this.m_context = copyFrom.m_context;
        }

        /// <summary>
        /// Fetch keys
        /// </summary>
        private IEnumerable<Guid> FetchKeys()
        {
            if (this.m_lastFetched == null)
            {
                this.m_lastFetched = this.m_provider.QueryPersistence.GetQueryResults(this.m_queryId, this.m_offset, this.m_count);
            }
            return this.m_lastFetched;
        }

        /// <summary>
        /// True if there is any results
        /// </summary>
        public bool Any()
        {
            return this.m_provider.QueryPersistence.QueryResultTotalQuantity(this.m_queryId) > 0;
        }

        /// <summary>
        /// Return this as a statefule query
        /// </summary>
        public IQueryResultSet<TData> AsStateful(Guid stateId)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Get the count of objects
        /// </summary>
        public int Count()
        {
            if (this.m_offset == 0 && this.m_count == 0)
            {
                return (int)this.m_provider.QueryPersistence.QueryResultTotalQuantity(this.m_queryId);
            }
            else
            {
                return this.FetchKeys().Count();
            }
        }

        /// <summary>
        /// Get the first instance of results
        /// </summary>
        /// <returns></returns>
        public TData First()
        {
            var retVal = this.FirstOrDefault();
            if (retVal == null)
            {
                throw new InvalidOperationException(ErrorMessages.SEQUENCE_NO_ELEMENTS);
            }
            return retVal;
        }

        /// <summary>
        /// Get the first object
        /// </summary>
        public TData FirstOrDefault()
        {
            try
            {
                this.m_context.Open();
                return this.m_provider.Get(this.m_context, this.FetchKeys().FirstOrDefault());
            }
            finally
            {
                this.m_context.Close();
            }
        }

        /// <summary>
        /// Get an enumerator of the results
        /// </summary>
        public IEnumerator<TData> GetEnumerator()
        {
            var offset = this.m_offset;
            try
            {
                this.m_context.Open();

                // TODO: Infinite rollthrough when there is no upper count bounds
                foreach (var res in this.FetchKeys())
                {
                    yield return this.m_provider.Get(this.m_context, res);
                }
            }
            finally
            {
                this.m_context.Close();
            }
        }

        /// <summary>
        /// Intersect with another result set
        /// </summary>
        public IQueryResultSet<TData> Intersect(Expression<Func<TData, bool>> query)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Intersect with another result set
        /// </summary>
        public IQueryResultSet<TData> Intersect(IQueryResultSet<TData> other)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Order by specified sort expression
        /// </summary>
        public IQueryResultSet<TData> OrderBy(Expression<Func<TData, dynamic>> sortExpression)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Order by specified expresion in descending order
        /// </summary>
        public IQueryResultSet<TData> OrderByDescending(Expression<Func<TData, dynamic>> sortExpression)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Return only one or throw
        /// </summary>
        /// <returns></returns>
        public TData Single()
        {
            var retVal = this.SingleOrDefault();
            if (retVal == null)
            {
                throw new InvalidOperationException(ErrorMessages.SEQUENCE_NO_ELEMENTS);
            }
            return retVal;
        }

        /// <summary>
        /// Fetch only one or the default
        /// </summary>
        public TData SingleOrDefault()
        {
            try
            {
                this.m_context.Open();
                return this.m_provider.Get(this.m_context, this.FetchKeys().SingleOrDefault());
            }
            finally
            {
                this.m_context.Close();
            }
        }

        /// <summary>
        /// Skip the number of results
        /// </summary>
        public IQueryResultSet<TData> Skip(int count)
        {
            return new MappedStatefulQueryResultSet<TData>(this)
            {
                m_offset = count,
                m_count = this.m_count
            };
        }

        /// <summary>
        /// Take the specified number of results
        /// </summary>
        public IQueryResultSet<TData> Take(int count)
        {
            return new MappedStatefulQueryResultSet<TData>(this)
            {
                m_offset = this.m_offset,
                m_count = count
            };
        }

        /// <summary>
        /// Union with another query
        /// </summary>
        public IQueryResultSet<TData> Union(Expression<Func<TData, bool>> query)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Union with another result set
        /// </summary>
        public IQueryResultSet<TData> Union(IQueryResultSet<TData> other)
        {
            if (other is MappedStatefulQueryResultSet<TData> tOther)
            {
                // Nab the query result sets and intersect them
                var queryUuid = Guid.NewGuid();
                var results = this.m_provider.QueryPersistence.GetQueryResults(tOther.m_queryId, 0, tOther.m_count)
                    .Union(this.m_provider.QueryPersistence.GetQueryResults(this.m_queryId, 0, this.m_count));
                this.m_provider.QueryPersistence.RegisterQuerySet(queryUuid, results, null, results.Count());
                this.m_provider.QueryPersistence.AbortQuerySet(this.m_queryId);
                this.m_provider.QueryPersistence.AbortQuerySet(tOther.m_queryId);
                return new MappedStatefulQueryResultSet<TData>(this.m_provider, this.m_queryId, results.Count());
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(MappedStatefulQueryResultSet<TData>), other.GetType()));
            }
        }

        /// <summary>
        /// Filter the result set
        /// </summary>
        public IQueryResultSet<TData> Where(Expression<Func<TData, bool>> query)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Get enumerator
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();

        /// <summary>
        /// Dispose of the result set
        /// </summary>
        public void Dispose()
        {
        }

        /// <summary>
        /// Non-generic version of where
        /// </summary>
        public IQueryResultSet Where(Expression query)
        {
            if (query is Expression<Func<TData, bool>> eq)
            {
                return this.Where(eq);
            }
            else
            {
                throw new ArgumentException(nameof(query), String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(Expression<Func<TData, bool>>), query.GetType()));
            }
        }

        /// <summary>
        /// Get the first object
        /// </summary>
        object IQueryResultSet.First() => this.First();

        /// <summary>
        /// Get the first or default (null) object
        /// </summary>
        object IQueryResultSet.FirstOrDefault() => this.FirstOrDefault();

        /// <summary>
        /// Get a single resut or throw
        /// </summary>
        object IQueryResultSet.Single() => this.Single();

        /// <summary>
        /// Get single result or default
        /// </summary>
        object IQueryResultSet.SingleOrDefault() => this.SingleOrDefault();

        /// <summary>
        /// Tag the specified <paramref name="count"/> objects
        /// </summary>
        IQueryResultSet IQueryResultSet.Take(int count) => this.Take(count);

        /// <summary>
        /// Skip the <paramref name="count"/> rsults
        /// </summary>
        IQueryResultSet IQueryResultSet.Skip(int count) => this.Skip(count);

        /// <summary>
        /// Represent as a stateful object
        /// </summary>
        IQueryResultSet IQueryResultSet.AsStateful(Guid stateId) => this.AsStateful(stateId);

        /// <summary>
        /// Select
        /// </summary>
        public IEnumerable<TReturn> Select<TReturn>(Expression<Func<TData, TReturn>> selector)
        {
            var compiled = selector.Compile();
            foreach (var i in this)
            {
                yield return compiled(i);
            }
        }

        /// <summary>
        /// Select the object according to a C# enumerator
        /// </summary>
        public IEnumerable<TReturn> Select<TReturn>(Func<TData, TReturn> selector)
        {
            foreach (var element in this)
            {
                yield return (TReturn)selector(element); ;
            }
        }

        public IQueryResultSet OrderBy(Expression expression)
        {
            throw new NotImplementedException();
        }

        public IQueryResultSet OrderByDescending(Expression expression)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Intersect this set with the other set
        /// </summary>
        public IQueryResultSet Intersect(IQueryResultSet other)
        {
            if (other is MappedStatefulQueryResultSet<TData> tother)
            {
                return this.Intersect(tother);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(MappedStatefulQueryResultSet<TData>), other.GetType()));
            }
        }

        /// <summary>
        /// Union this set with other
        /// </summary>
        public IQueryResultSet Union(IQueryResultSet other)
        {
            if (other is MappedStatefulQueryResultSet<TData> tother)
            {
                return this.Union(tother);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(MappedStatefulQueryResultSet<TData>), other.GetType()));
            }
        }

        /// <summary>
        /// Return only those results
        /// </summary>
        /// <typeparam name="TType"></typeparam>
        /// <returns></returns>
        public IEnumerable<TType> OfType<TType>()
        {
            throw new NotImplementedException();
        }
    }
}