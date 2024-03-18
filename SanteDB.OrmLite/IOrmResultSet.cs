/*
 * Copyright (C) 2021 - 2024, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-6-21
 */
using System;
using System.Collections;
using System.Linq.Expressions;

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
        /// Gets the data context
        /// </summary>
        DataContext Context { get; }

        /// <summary>
        /// Gets the type of data in the property
        /// </summary>
        Type ElementType { get; }

        /// <summary>
        /// Clone on a new context
        /// </summary>
        IOrmResultSet CloneOnContext(DataContext context);

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
        /// Returns only those objects in the result set whos primary keys are listed 
        /// </summary>
        IOrmResultSet HavingKeys(IEnumerable keys, String keyColumnName);

        /// <summary>
        /// Get first or default item in collection
        /// </summary>
        Object FirstOrDefault();

        /// <summary>
        /// Order in ascending order accoridng to expression
        /// </summary>
        IOrmResultSet OrderBy(LambdaExpression orderExpression);

        /// <summary>
        /// Order by descending according to expression
        /// </summary>
        /// <param name="orderExpression"></param>
        /// <returns></returns>
        IOrmResultSet OrderByDescending(LambdaExpression orderExpression);

        /// <summary>
        /// Wraps the query in this result set in another query with  a where clause matching <paramref name="whereExpression"/>
        /// </summary>
        IOrmResultSet Where(Expression whereExpression);


        /// <summary>
        /// Except keys
        /// </summary>
        IOrmResultSet Except(IOrmResultSet other);

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

        /// <summary>
        /// Remove the odering instructions
        /// </summary>
        IOrmResultSet WithoutOrdering(out SqlStatement orderByStatement);

        /// <summary>
        /// Remove the skip instructions
        /// </summary>
        IOrmResultSet WithoutSkip(out int originalOffset);

        /// <summary>
        /// Remove the take instruction
        /// </summary>
        IOrmResultSet WithoutTake(out int originalTake);

        /// <summary>
        /// Clone the current result set with the specified sqlstatement
        /// </summary>
        IOrmResultSet Clone(SqlStatement statement);
    }
}