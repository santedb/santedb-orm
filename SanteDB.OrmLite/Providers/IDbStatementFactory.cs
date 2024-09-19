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
 */
using System;
using System.Collections.Generic;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents a class which can create statements for various providers
    /// </summary>
    public interface IDbStatementFactory
    {

        /// <summary>
        /// Get the invariant
        /// </summary>
        string Invariant { get; }

        /// <summary>
        /// Get the SQL server engine features
        /// </summary>
        SqlEngineFeatures Features { get; }

        /// <summary>
        /// Creates an Exists statement
        /// </summary>
        SqlStatement Count(SqlStatement sqlStatement);

        /// <summary>
        /// Creates an Exists statement
        /// </summary>
        SqlStatement Exists(SqlStatement sqlStatement);

        /// <summary>
        /// Appends a RETURNING statement
        /// </summary>
        SqlStatement Returning(params ColumnMapping[] returnColumns);

        /// <summary>
        /// Gets the reset sequence command
        /// </summary>
        SqlStatement GetResetSequence(string sequenceName, object sequenceValue);

        /// <summary>
        /// Create the statement to define the index
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="column">The column to be indexed</param>
        /// <param name="tableName">The table to be indexed</param>
        /// <param name="isUnique">True if the index is uique</param>
        SqlStatement CreateIndex(String indexName, String tableName, String column, bool isUnique);

        /// <summary>
        /// Create the statement to drop the specified index
        /// </summary>
        /// <param name="indexName">The index name</param>
        SqlStatement DropIndex(String indexName);

        /// <summary>
        /// Get the next sequence value for the specified sequence
        /// </summary>
        SqlStatement GetNextSequenceValue(String sequenceName);

        /// <summary>
        /// Create SQL keyword
        /// </summary>
        String CreateSqlKeyword(SqlKeyword keywordType);

        /// <summary>
        /// Gets the specified filter function
        /// </summary>
        /// <param name="name">The name of the filter function to retrieve</param>
        /// <returns>The retrieved filter function if it is provided by the provider</returns>
        IDbFilterFunction GetFilterFunction(String name);

        /// <summary>
        /// Get all filter functions
        /// </summary>
        IEnumerable<IDbFilterFunction> GetFilterFunctions();

        /// <summary>
        /// Gets the provider this statement factory belongs to
        /// </summary>
        IDbProvider Provider { get; }
    }
}
