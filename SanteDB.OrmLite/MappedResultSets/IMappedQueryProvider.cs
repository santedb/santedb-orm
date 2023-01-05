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
using SanteDB.Core.Services;
using SanteDB.OrmLite.Providers;
using System;
using System.Linq.Expressions;

namespace SanteDB.OrmLite.MappedResultSets
{
    /// <summary>
    /// Represents an ADO query provider which is used by wrapped result set
    /// instance to calculate its result set.
    /// </summary>
    public interface IMappedQueryProvider<TModel>
    {
        /// <summary>
        /// Get the provider
        /// </summary>
        IDbProvider Provider { get; }

        /// <summary>
        /// Gets the instance of the query persistence service
        /// </summary>
        IQueryPersistenceService QueryPersistence { get; }

        /// <summary>
        /// Execute a query for the specified model with query identifier returning the results
        /// </summary>
        /// <param name="query">The query to execute</param>
        /// <returns>The amount of results to return</returns>
        IOrmResultSet ExecuteQueryOrm(DataContext context, Expression<Func<TModel, bool>> query);

        /// <summary>
        /// Get the specified object at the specified key
        /// </summary>
        TModel Get(DataContext context, Guid key);

        /// <summary>
        /// Convert <paramref name="result"/> to model
        /// </summary>
        TModel ToModelInstance(DataContext context, object result);

        /// <summary>
        /// Map sort expression
        /// </summary>
        /// <param name="sortExpression">The sort expression to map</param>
        /// <returns>The mapped sort expression</returns>
        Expression MapExpression<TReturn>(Expression<Func<TModel, TReturn>> sortExpression);

        /// <summary>
        /// Apply any special versioning filters on the specified statement
        /// </summary>
        /// <param name="tableAlias">The table alias to use</param>
        /// <returns>The updated statement with versioning filters</returns>
        SqlStatement GetCurrentVersionFilter(String tableAlias);
    }
}