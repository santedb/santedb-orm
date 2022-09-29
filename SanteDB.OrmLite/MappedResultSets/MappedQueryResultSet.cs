/*
 * Copyright (C) 2021 - 2022, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
 * Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors
 * Portions Copyright (C) 2015-2018 Mohawk College of Applied Arts and Technology
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. You may 
 * obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations under 
 * the License.
 * 
 * User: fyfej
 * Date: 2022-9-7
 */
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Services;
using SanteDB.OrmLite;
using SanteDB.Core.i18n;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Reflection;

namespace SanteDB.OrmLite.MappedResultSets
{
    /// <summary>
    /// Represents an Ado Persistence query set
    /// </summary>
    /// <remarks>This query set wraps the returns of Query methods and allows for
    /// delay loading of the resulting data from the underlying data provider</remarks>
    public class MappedQueryResultSet<TElement> : IQueryResultSet<TElement>, IOrderableQueryResultSet<TElement>, IDisposable
        where TElement : IdentifiedData
    {

#if DEBUG
        private int m_expansionCount = 0;
#endif 

        // The data context
        private readonly DataContext m_context;

        // The query provider
        private readonly IMappedQueryProvider<TElement> m_provider;

        // The result set that this wraps
        private IOrmResultSet m_resultSet;

        // Tracer
        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(MappedQueryResultSet<TElement>));

        // The key name to use for stateful storage
        private readonly string m_keyName;

        // true if context should be kept open
        private readonly bool m_keepContextOpen;

        /// <summary>
        /// State key name
        /// </summary>
        protected String StateKeyName => this.m_keyName;

        /// <summary>
        /// Get the result set
        /// </summary>
        protected IOrmResultSet ResultSet => this.m_resultSet;

        /// <summary>
        /// Get the provider for this set
        /// </summary>
        protected IMappedQueryProvider<TElement> Provider => this.m_provider;


        /// <summary>
        /// Creates a new persistence collection
        /// </summary>
        public MappedQueryResultSet(IMappedQueryProvider<TElement> dataProvider)
        {
            this.m_provider = dataProvider;
            this.m_context = dataProvider.Provider.GetReadonlyConnection();
        }

        /// <summary>
        /// Creates a new persistence collection
        /// </summary>
        public MappedQueryResultSet(IMappedQueryProvider<TElement> dataProvider, String stateKeyName)
        {
            this.m_provider = dataProvider;
            this.m_context = dataProvider.Provider.GetReadonlyConnection();
            this.m_keyName = stateKeyName;
        }

        /// <summary>
        /// Create mapped query result set
        /// </summary>
        public MappedQueryResultSet(IMappedQueryProvider<TElement> dataProvider, DataContext context)
        {
            this.m_provider = dataProvider;
            this.m_context = context;
            this.m_keepContextOpen = true;
        }

        /// <summary>
        /// Create mappe query result set from SQL result set
        /// </summary>
        public MappedQueryResultSet(IMappedQueryProvider<TElement> provider, IOrmResultSet resultSet, bool keepContextOpen = false)
        {
            this.m_provider = provider;
            this.m_context = resultSet.Context;
            this.m_resultSet = resultSet;
            this.m_keepContextOpen = keepContextOpen;
        }

        /// <summary>
        /// Create a wrapper persistence collection
        /// </summary>
        private MappedQueryResultSet(MappedQueryResultSet<TElement> copyFrom, IOrmResultSet resultSet)
        {
            this.m_provider = copyFrom.m_provider;
            this.m_resultSet = resultSet;
            this.m_context = copyFrom.m_context;
            this.m_keyName = copyFrom.m_keyName;
            this.m_keepContextOpen = copyFrom.m_keepContextOpen;
        }

        /// <summary>
        /// Clone with the specified result set
        /// </summary>
        protected virtual MappedQueryResultSet<TElement> CloneWith(IOrmResultSet resultSet)
        {
            return new MappedQueryResultSet<TElement>(this, resultSet);
        }

        /// <summary>
        /// Prepare the result set for execution
        /// </summary>
        protected virtual IOrmResultSet PrepareResultSet()
        {
            if (this.m_resultSet == null)
            {
                this.m_resultSet = this.m_provider.ExecuteQueryOrm(this.m_context, o => true);
            }
            return this.m_resultSet;
        }
        /// <summary>
        /// Where clause for filtering on provider
        /// </summary>
        public virtual IQueryResultSet<TElement> Where(Expression<Func<TElement, bool>> query)
        {
            if (this.m_resultSet != null)
            {
                // This is in effect an intersect
                return this.CloneWith(this.m_resultSet.Where(this.m_provider.MapExpression<bool>(query)));
            }
            else
            {
                return this.CloneWith(this.m_provider.ExecuteQueryOrm(this.m_context, query));
            }
        }

        /// <summary>
        /// Execute the specified SQL statement
        /// </summary>
        public virtual IQueryResultSet<TElement> Execute<TDBModel>(SqlStatement statement)
        {
            if (this.m_resultSet != null)
            {
                // This is in effect an intersect
                return this.CloneWith(this.m_resultSet.Intersect(this.m_context.Query<TDBModel>(statement)));
            }
            else
            {
                return this.CloneWith(this.m_context.Query<TDBModel>(statement));
            }
        }

        /// <summary>
        /// Union the results in this query set with those in another
        /// </summary>
        public virtual IQueryResultSet<TElement> Union(Expression<Func<TElement, bool>> query)
        {
            if (this.m_resultSet == null) // this is the first
            {
                return this.CloneWith(this.m_provider.ExecuteQueryOrm(this.m_context, query));
            }
            else
            {
                return this.CloneWith(this.m_resultSet.Union(this.m_provider.ExecuteQueryOrm(this.m_context, query)));
            }
        }

        /// <summary>
        /// Intersect with another dataset
        /// </summary>
        public virtual IQueryResultSet<TElement> Intersect(Expression<Func<TElement, bool>> query)
        {
            if (this.m_resultSet == null) // this is the first
            {
                return this.CloneWith(this.m_provider.ExecuteQueryOrm(this.m_context, query));
            }
            else
            {
                return this.CloneWith(this.m_resultSet.Intersect(this.m_provider.ExecuteQueryOrm(this.m_context, query)));
            }
        }

        /// <summary>
        /// Skip the specified number of elements
        /// </summary>
        public virtual IQueryResultSet<TElement> Skip(int count)
        {
            if (this.m_resultSet == null) // this is the first
            {
                this.m_resultSet = this.m_provider.ExecuteQueryOrm(this.m_context, o => true);
            }

            return this.CloneWith(this.m_resultSet.Skip(count));
        }

        /// <summary>
        /// Flattens the object into an enumerator
        /// </summary>
        public virtual IEnumerator<TElement> GetEnumerator()
        {
            var execResultSet = this.PrepareResultSet();
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            this.m_expansionCount++;
            if (this.m_expansionCount > 1)
            {
                this.m_tracer.TraceWarning("QUERY RESULT SET {0} HAS BEEN EXPANDED {1} TIMES AT {2}", execResultSet.ToSqlStatement().SQL, this.m_expansionCount, new StackTrace());
            }
#endif
            try
            {
                this.m_context.Open();
                using (var subContext = this.m_context.OpenClonedContext()) // Sub context is used for loading of dynamic properties 
                {
                    subContext.Open();
                    foreach (var result in execResultSet)
                    {
                        yield return this.m_provider.ToModelInstance(subContext, result);
                    }
                }
            }
            finally
            {
                if (!this.m_keepContextOpen)
                {
                    this.m_context.Close();
                }
#if DEBUG
                sw.Stop();
                this.m_tracer.TraceVerbose("Performance: GetEnumerator({0}) took {1}ms", execResultSet, sw.ElapsedMilliseconds);
#endif
            }
        }

        /// <summary>
        /// Get enumerator for generic instances
        /// </summary>
        /// <returns></returns>
        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();

        /// <summary>
        /// Get the first instance of results
        /// </summary>
        public TElement First()
        {
            var retVal = this.FirstOrDefault();
            if (retVal == null)
            {
                throw new InvalidOperationException(ErrorMessages.SEQUENCE_NO_ELEMENTS);
            }
            return retVal;
        }

        /// <summary>
        /// Get the first or default of the result
        /// </summary>
        public virtual TElement FirstOrDefault()
        {
            var execResultSet = this.PrepareResultSet();
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
#endif
            try
            {
                this.m_context.Open();

                return this.m_provider.ToModelInstance(this.m_context, execResultSet.FirstOrDefault());
            }
            finally
            {
                if (!this.m_keepContextOpen)
                {
                    this.m_context.Close();
                }
#if DEBUG
                sw.Stop();
                this.m_tracer.TraceVerbose("Performance: SingleOrDefault({0}) took {1}ms", execResultSet, sw.ElapsedMilliseconds);
#endif
            }
        }

        /// <summary>
        /// Get only one object
        /// </summary>
        public virtual TElement Single()
        {
            var retVal = this.SingleOrDefault();
            if (retVal == null)
            {
                throw new InvalidOperationException(ErrorMessages.SEQUENCE_NO_ELEMENTS);
            }
            return retVal;
        }

        /// <summary>
        /// Get only one of the objects
        /// </summary>
        public virtual TElement SingleOrDefault()
        {
            var execResultSet = this.PrepareResultSet();
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
#endif
            try
            {
                this.m_context.Open();

                var resultCount = execResultSet.Count();
                if (resultCount <= 1)
                {
                    return this.m_provider.ToModelInstance(this.m_context, execResultSet.FirstOrDefault());
                }
                else
                {
                    throw new InvalidOperationException(ErrorMessages.SEQUENCE_MORE_THAN_ONE);
                }
            }
            finally
            {
                if (!this.m_keepContextOpen)
                {
                    this.m_context.Close();
                }
#if DEBUG
                sw.Stop();
                this.m_tracer.TraceVerbose("Performance: SingleOrDefault({0}) took {1}ms", execResultSet, sw.ElapsedMilliseconds);
#endif
            }
        }

        /// <summary>
        /// Union this dataset with another
        /// </summary>
        public virtual IQueryResultSet<TElement> Union(IQueryResultSet<TElement> other)
        {
            if (other is MappedQueryResultSet<TElement> otherStrong)
            {
                if (this.m_resultSet != null && otherStrong.m_resultSet != null)
                {
                    return this.CloneWith(this.PrepareResultSet().Union(otherStrong.PrepareResultSet()));
                }
                else
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Union)));
                }
            }
            else
            {
                throw new ArgumentOutOfRangeException(String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(MappedQueryResultSet<>), other.GetType()));
            }
        }

        /// <summary>
        /// Takes the number of objects from the result set
        /// </summary>
        public virtual IQueryResultSet<TElement> Take(int count)
        {
            if (this.m_resultSet == null)
            {
                this.m_resultSet = this.m_provider.ExecuteQueryOrm(this.m_context, o => true);
            }
            return this.CloneWith(this.m_resultSet.Take(count));
        }

        /// <summary>
        /// Append the order by statements onto the specifed result set
        /// </summary>
        public virtual IOrderableQueryResultSet<TElement> OrderBy<TKey>(Expression<Func<TElement, TKey>> sortExpression)
        {
            if (this.m_resultSet == null) // this is the first
            {
                this.m_resultSet = this.m_provider.ExecuteQueryOrm(this.m_context, o => true);
            }

            return this.CloneWith(this.m_resultSet.OrderBy(this.m_provider.MapExpression(sortExpression)));
        }

        /// <summary>
        /// Order result set by descending order
        /// </summary>
        public virtual IOrderableQueryResultSet<TElement> OrderByDescending<TKey>(Expression<Func<TElement, TKey>> sortExpression)
        {
            if (this.m_resultSet == null) // this is the first
            {
                this.m_resultSet = this.m_provider.ExecuteQueryOrm(this.m_context, o => true);
            }

            return this.CloneWith(this.m_resultSet.OrderByDescending(this.m_provider.MapExpression(sortExpression)));
        }

        /// <summary>
        /// Creates a query set or loads the specified query set
        /// </summary>
        public virtual IQueryResultSet<TElement> AsStateful(Guid stateId)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
#endif
            try
            {
                if (this.m_provider.QueryPersistence == null)
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.DEPENDENT_PROPERTY_NULL, nameof(IMappedQueryProvider<TElement>.QueryPersistence)));
                }

                // Is the query already registered? If so, load
                if (this.m_provider.QueryPersistence.IsRegistered(stateId))
                {
                    return new MappedStatefulQueryResultSet<TElement>(this.m_provider, stateId, (int)this.m_provider.QueryPersistence.QueryResultTotalQuantity(stateId), this.m_resultSet, this.m_keyName);
                }
                else
                {
                    this.m_context.Open();

                    Guid[] keySet = null;
                    if (!String.IsNullOrEmpty(this.m_keyName))
                    {
                        // TODO: In Firebird this seems to lose ordering?
                        keySet = this.m_resultSet.Select<Guid>(this.m_keyName).ToArray().Distinct().ToArray();
                    }
                    else
                    {
                        keySet = this.m_resultSet.Keys<Guid>().OfType<Guid>().ToArray();
                    }
                    this.m_provider.QueryPersistence.RegisterQuerySet(stateId, keySet, this.m_resultSet.Statement, keySet.Length);
                    return new MappedStatefulQueryResultSet<TElement>(this.m_provider, stateId, keySet.Length, this.m_resultSet, this.m_keyName);
                }
            }
            finally
            {
                if (!this.m_keepContextOpen)
                {

                    this.m_context.Close();
                }
#if DEBUG
                sw.Stop();
                this.m_tracer.TraceVerbose("Performance: AsStateful({0}) took {1}ms", stateId, sw.ElapsedMilliseconds);
#endif
            }
        }

        /// <summary>
        /// Return true if there are any matching reuslts
        /// </summary>
        public virtual bool Any()
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
#endif
            var execResultSet = this.PrepareResultSet();
            try
            {
                this.m_context.Open();
                return execResultSet.Any();
            }
            finally
            {
                if (!this.m_keepContextOpen)
                {

                    this.m_context.Close();
                }
            }
        }

        /// <summary>
        /// Get the count of objects
        /// </summary>
        public int Count()
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
#endif
            var execResultSet = this.PrepareResultSet();
            try
            {
                this.m_context.Open();
                return execResultSet.Count();
            }
            finally
            {
                if (!this.m_keepContextOpen)
                {

                    this.m_context.Close();
                }
            }
        }

        /// <summary>
        /// Intersect with another result set
        /// </summary>
        public virtual IQueryResultSet<TElement> Intersect(IQueryResultSet<TElement> other)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Dispose of this object
        /// </summary>
        public virtual void Dispose()
        {
            if (!this.m_keepContextOpen)
            {
                this.m_context.Dispose();
            }
        }

        /// <summary>
        /// Non-generic version of where
        /// </summary>
        public virtual IQueryResultSet Where(Expression query)
        {
            if (query is Expression<Func<TElement, bool>> eq)
            {
                return this.Where(eq);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(query), String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(Expression<Func<TElement, bool>>), query.GetType()));
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
        /// Non-generic select method
        /// </summary>
        public virtual IEnumerable<TReturn> Select<TReturn>(Expression selector)
        {
            if (selector is Expression<Func<TElement, TReturn>> se)
            {
                return this.Select(se);
            }
            else if (selector is Expression<Func<TElement, dynamic>> de)
            {
                // Strip body convert
                return this.Select(Expression.Lambda<Func<TElement, TReturn>>(Expression.Convert(de.Body, typeof(TReturn)).Reduce(), de.Parameters));
            }
            else
            {
                throw new NotSupportedException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(Expression<Func<TElement, TReturn>>), selector.GetType()));
            }
        }

        /// <summary>
        /// Select the specified objects from the database
        /// </summary>
        public IEnumerable<TReturn> Select<TReturn>(Expression<Func<TElement, TReturn>> selector)
        {
            var execResultSet = this.PrepareResultSet();

            var member = this.m_provider.MapExpression(selector).GetMember();
            try
            {
                this.m_context.Open();
                foreach (var element in execResultSet.Select<TReturn>(member.Name))
                {
                    yield return element;
                }
            }
            finally
            {
                if (!this.m_keepContextOpen)
                {
                    this.m_context.Close();
                }
            }
        }

        /// <summary>
        /// Select the specifed objects with a C# expression
        /// </summary>
        /// <remarks>This flattens or executes the query</remarks>
        public virtual IEnumerable<TReturn> Select<TReturn>(Func<TElement, TReturn> selector)
        {
            // Flatten
            foreach (var element in this)
            {
                yield return (TReturn)selector(element);
            }
        }

        /// <summary>
        /// Order by a generic expression
        /// </summary>
        public virtual IOrderableQueryResultSet OrderBy(Expression expression)
        {
            if (expression is Expression<Func<TElement, dynamic>> le)
            {
                return this.OrderBy(le);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, typeof(Expression<Func<TElement, dynamic>>), expression.GetType()));
            }
        }

        /// <summary>
        /// Order by descending order
        /// </summary>
        public virtual IOrderableQueryResultSet OrderByDescending(Expression expression)
        {
            if (expression is Expression<Func<TElement, dynamic>> strong)
            {
                return this.OrderByDescending(strong);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, typeof(Expression<Func<TElement, dynamic>>), expression.GetType()));
            }
        }

        /// <summary>
        /// Intersect the other result set - note this can only be of same type of set
        /// </summary>
        public virtual IQueryResultSet Intersect(IQueryResultSet other)
        {
            if (other is MappedQueryResultSet<TElement> mq)
            {
                return this.Intersect(mq);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(other), String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(MappedQueryResultSet<TElement>), other.GetType()));
            }
        }

        /// <summary>
        /// Union the other result set - note this can only be of the same type of set
        /// </summary>
        public virtual IQueryResultSet Union(IQueryResultSet other)
        {
            if (other is MappedQueryResultSet<TElement> mq)
            {
                return this.Union(mq);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(other), String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(MappedQueryResultSet<TElement>), other.GetType()));
            }
        }

        /// <summary>
        /// Return only those results in the result set which are of type <typeparamref name="TType"/>
        /// </summary>
        public virtual IEnumerable<TType> OfType<TType>()
        {
            foreach (var itm in this)
            {
                if (itm is TType typ)
                {
                    yield return typ;
                }
            }
        }
    }
}