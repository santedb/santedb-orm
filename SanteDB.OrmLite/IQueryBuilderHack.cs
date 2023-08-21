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
 * Date: 2023-5-19
 */
using System;
using System.Collections.Generic;
using System.Reflection;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Optimizes specific query scenarios for performance or consistent syntax.
    /// </summary>
    public interface IQueryBuilderHack
    {

        /// <summary>
        /// Hacks the query in some manner.
        /// </summary>
        /// <param name="builder">The query builder that will be used to assemble a hacked WHERE clause.</param>
        /// <param name="sqlStatementBuilder">The current vanilla (no WHERE clause) query.</param>
        /// <param name="whereClause">The current where clause</param>
        /// <param name="tmodel">The type being queried.</param>
        /// <param name="property">The property which is currently being hacked</param>
        /// <param name="queryPrefix">The prefix appended to any identifiers in the query. In a database, this will often be the schema or database name.</param>
        /// <param name="predicate">The current predicate</param>
        /// <param name="values">The values to use with the comparsion that this hack generates.</param>
        /// <param name="scopedTables">The tables that are scoped for the current query</param>
        /// <param name="queryFilter"></param>
        /// <returns>True if the <paramref name="whereClause"/> was modified by this hack, False otherwise.</returns>
        bool HackQuery(QueryBuilder builder, SqlStatementBuilder sqlStatementBuilder, SqlStatementBuilder whereClause, Type tmodel, PropertyInfo property, String queryPrefix, QueryPredicate predicate, String[] values, IEnumerable<TableMapping> scopedTables, IDictionary<String, String[]> queryFilter);

    }
}
