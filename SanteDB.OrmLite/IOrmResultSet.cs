﻿/*
 * Copyright (C) 2021 - 2021, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-8-5
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Non-generic interface
    /// </summary>
    public interface IOrmResultSet : IEnumerable
    {
        /// <summary>
        /// Gets the SQL statement that this result set is based on
        /// </summary>
        SqlStatement Statement { get; }

        /// <summary>
        /// Gets the type of data in the property
        /// </summary>
        Type ElementType { get; }

        /// <summary>
        /// Counts the number of records
        /// </summary>
        int Count();

        /// <summary>
        /// Return only distinct objects
        /// </summary>
        IOrmResultSet Distinct();

        /// <summary>
        /// Skip N results
        /// </summary>
        IOrmResultSet Skip(int count);

        /// <summary>
        /// Take N results
        /// </summary>
        IOrmResultSet Take(int count);

        /// <summary>
        /// Gets the specified key
        /// </summary>
        IOrmResultSet Keys<TKey>();

        /// <summary>
        /// Convert this result set to an SQL statement
        /// </summary>
        SqlStatement ToSqlStatement();

        /// <summary>
        /// Get first or default item in collection
        /// </summary>
        Object FirstOrDefault();

        /// <summary>
        /// Order in ascending order accoridng to expression
        /// </summary>
        IOrmResultSet OrderBy(Expression orderExpression);

        /// <summary>
        /// Order by descending according to expression
        /// </summary>
        /// <param name="orderExpression"></param>
        /// <returns></returns>
        IOrmResultSet OrderByDescending(Expression orderExpression);

        /// <summary>
        /// Union keys
        /// </summary>
        IOrmResultSet Union(IOrmResultSet other);

        /// <summary>
        /// Intersect keys
        /// </summary>
        IOrmResultSet Intersect(IOrmResultSet other);

        /// <summary>
        /// Select only keys
        /// </summary>
        OrmResultSet<TKey> Keys<TKey>(bool qualifyTables = true);

        /// <summary>
        /// True if there are any matching
        /// </summary>
        bool Any();

        /// <summary>
        /// Select the
        /// </summary>
        /// <param name="property">The fields to be selected</param>
        OrmResultSet<TElement> Select<TElement>(string property);
    }
}