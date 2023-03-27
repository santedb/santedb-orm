/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-3-10
 */
using SanteDB.BI.Rendering;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.i18n;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace SanteDB.OrmLite.MappedResultSets
{
    /// <summary>
    /// Represents a stateful result set
    /// </summary>
    public class MappedStatefulQueryResultSet<TData> : MappedQueryResultSet<TData>, IDisposable
        where TData : IIdentifiedResource
    {

        // Get tracer
        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(MappedStatefulQueryResultSet<TData>));
        // Query id
        private readonly Guid m_queryId;

        /// <summary>
        /// Creates a new persistence collection
        /// </summary>
        private MappedStatefulQueryResultSet(MappedStatefulQueryResultSet<TData> copyFrom, IOrmResultSet resultSet) : base(copyFrom, resultSet)
        {
            this.m_queryId = copyFrom.m_queryId;
        }

        /// <summary>
        /// Mapped stateful query result set
        /// </summary>
        /// <remarks>Any ordering is removed from <paramref name="resultSet"/> since it is expected the <paramref name="queryId"/> is sorted and restricted</remarks>
        internal MappedStatefulQueryResultSet(MappedQueryResultSet<TData> copyFrom, IOrmResultSet resultSet, Guid queryId) : base(copyFrom, resultSet.WithoutOrdering().WithoutSkip(out _).WithoutTake(out _))
        {
            this.m_queryId = queryId;
        }

        /// <summary>
        /// Clone this result set with the specified result set
        /// </summary>
        protected override MappedQueryResultSet<TData> CloneWith(IOrmResultSet resultSet)
        {
            return new MappedStatefulQueryResultSet<TData>(this, resultSet);
        }

        /// <inheritdoc/>
        public override bool Any()
        {
            this.ResultSet.WithoutSkip(out var offset).WithoutTake(out var limit);
            if (limit < 0)
            {
                return this.Provider.QueryPersistence.QueryResultTotalQuantity(this.m_queryId) > offset;
            }
            else
            {
                return this.Provider.QueryPersistence.QueryResultTotalQuantity(this.m_queryId) - limit > offset ;
            }
        }

        /// <summary>
        /// Count the results
        /// </summary>
        public override int Count()
        {
            this.ResultSet.WithoutSkip(out var offset).WithoutTake(out var limit);
            if (limit < 0)
            {
                return (int)this.Provider.QueryPersistence.QueryResultTotalQuantity(this.m_queryId) - offset;
            }
            else
            {
                return this.Provider.QueryPersistence.GetQueryResults(this.m_queryId, offset, limit).Count();
            }
        }

        /// <summary>
        /// Prepare the result set
        /// </summary>
        protected override IOrmResultSet PrepareResultSet(IOrmResultSet resultSet)
        {
            // Fetch the keys according to the 
            var retVal = resultSet.WithoutOrdering().WithoutSkip(out var offset).WithoutTake(out var limit);

            // No limit query
            if (limit < 0)
            {
                this.m_tracer.TraceWarning("No limit has been specified - this may take a while to construct the stateful query load! Consider calling .Take() before executing this method");
                limit = (int)this.Provider.QueryPersistence.QueryResultTotalQuantity(this.m_queryId);
            }

            // Fetch the result set
            var guids = this.Provider.QueryPersistence.GetQueryResults(this.m_queryId, offset, limit);

            retVal = retVal.HavingKeys(guids, this.StateKeyName);
            var currentVersionFilter = this.Provider.GetCurrentVersionFilter(retVal.Statement.Alias); // Multiple rows may have the same id
            if (currentVersionFilter != null)
            {
                return retVal.Clone(retVal.Statement.Append(" WHERE ").Append(currentVersionFilter));
            }
            else
            {
                return retVal;
            }
        }


        /// <inheritdoc/>
        /// <exception cref="NotSupportedException">Ordering is not supported on stateful queries</exception>
        public override IOrderableQueryResultSet<TData> OrderBy<TKey>(Expression<Func<TData, TKey>> sortExpression)
        {
            throw new NotSupportedException(ErrorMessages.NOT_SUPPORTED);
        }

        /// <inheritdoc/>
        /// <exception cref="NotSupportedException">Ordering is not supported on stateful queries</exception>
        public override IOrderableQueryResultSet<TData> OrderByDescending<TKey>(Expression<Func<TData, TKey>> sortExpression)
        {
            throw new NotSupportedException(ErrorMessages.NOT_SUPPORTED);
        }


        /// <inheritdoc/>
        /// <exception cref="InvalidOperationException">Stateful result sets are already stateful</exception>
        public override IQueryResultSet<TData> AsStateful(Guid stateId)
        {
            throw new InvalidOperationException(String.Format(ErrorMessages.MULTIPLE_CALLS_NOT_ALLOWED, nameof(AsStateful)));
        }

        /// <summary>
        /// Union the query result set
        /// </summary>
        public override IQueryResultSet<TData> Intersect(IQueryResultSet<TData> other)
        {
            if (other is MappedStatefulQueryResultSet<TData> tOther)
            {
                // Nab the query result sets and intersect them
                var queryUuid = Guid.NewGuid();
                var results = this.Provider.QueryPersistence.GetQueryResults(tOther.m_queryId, 0, (int)tOther.Provider.QueryPersistence.QueryResultTotalQuantity(tOther.m_queryId))
                                    .Intersect(this.Provider.QueryPersistence.GetQueryResults(this.m_queryId, 0, (int)this.Provider.QueryPersistence.QueryResultTotalQuantity(this.m_queryId)));
                this.Provider.QueryPersistence.RegisterQuerySet(queryUuid, results, null, results.Count());
                this.Provider.QueryPersistence.AbortQuerySet(this.m_queryId);
                this.Provider.QueryPersistence.AbortQuerySet(tOther.m_queryId);
                return new MappedStatefulQueryResultSet<TData>(this, this.ResultSet, queryUuid);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(MappedStatefulQueryResultSet<TData>), other.GetType()));
            }
        }

        /// <summary>
        /// Union the query result set
        /// </summary>
        public override IQueryResultSet<TData> Union(IQueryResultSet<TData> other)
        {
            if (other is MappedStatefulQueryResultSet<TData> tOther)
            {
                // Nab the query result sets and intersect them
                var queryUuid = Guid.NewGuid();
                var results = this.Provider.QueryPersistence.GetQueryResults(tOther.m_queryId, 0, (int)tOther.Provider.QueryPersistence.QueryResultTotalQuantity(tOther.m_queryId))
                                    .Union(this.Provider.QueryPersistence.GetQueryResults(this.m_queryId, 0, (int)this.Provider.QueryPersistence.QueryResultTotalQuantity(this.m_queryId)));
                this.Provider.QueryPersistence.RegisterQuerySet(queryUuid, results, null, results.Count());
                this.Provider.QueryPersistence.AbortQuerySet(this.m_queryId);
                this.Provider.QueryPersistence.AbortQuerySet(tOther.m_queryId);
                return new MappedStatefulQueryResultSet<TData>(this, this.ResultSet, queryUuid);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(MappedStatefulQueryResultSet<TData>), other.GetType()));
            }
        }
    }
}