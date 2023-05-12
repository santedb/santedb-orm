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
using SanteDB.Core;
using SanteDB.Core.Services;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace SanteDB.OrmLite.Providers.Firebird
{
    /// <summary>
    /// Statement factory for FirebirdSQL
    /// </summary>
    public class FirebirdStatementFactory : IDbStatementFactory
    {
        private static readonly ConcurrentDictionary<string, IDbFilterFunction> s_filterFunctions;

        /// <summary>
        /// Static CTOR
        /// </summary>
        static FirebirdStatementFactory()
        {
            if (ApplicationServiceContext.Current != null)
            {
                s_filterFunctions = new ConcurrentDictionary<string, IDbFilterFunction>(ApplicationServiceContext.Current.GetService<IServiceManager>()
                    .CreateInjectedOfAll<IDbFilterFunction>()
                    .Where(o => o.Provider == FirebirdSQLProvider.InvariantName)
                    .ToDictionary(o => o.Name, o => o));
            }
            else
            {
                s_filterFunctions = new ConcurrentDictionary<string, IDbFilterFunction>();
            }

        }

        /// <summary>
        /// Gets the filter function
        /// </summary>
        public IDbFilterFunction GetFilterFunction(string name)
        {
            s_filterFunctions.TryGetValue(name, out var retVal);
            return retVal;
        }

        /// <summary>
        /// Perform a returning command
        /// </summary>
        /// <param name="returnColumns">The columns to return</param>
        /// <returns>The returned colums</returns>
        public SqlStatement Returning(params ColumnMapping[] returnColumns)
        {
            if (returnColumns.Length == 0)
            {
                return SqlStatement.Empty;
            }

            return new SqlStatement($" RETURNING {String.Join(",", returnColumns.Select(o => $"{o.Name}"))}");
        }



        /// <inheritdoc/>
        public String Invariant => FirebirdSQLProvider.InvariantName;

        /// <summary>
        /// Gets the features that this provider
        /// </summary>
        public SqlEngineFeatures Features
        {
            get
            {
                return SqlEngineFeatures.AutoGenerateTimestamps |
                    SqlEngineFeatures.FetchOffset |
                    SqlEngineFeatures.AutoGenerateSequences |
                    SqlEngineFeatures.ReturnedInsertsAsParms |
                    SqlEngineFeatures.StrictSubQueryColumnNames;
            }
        }

        /// <summary>
        /// Turn the specified SQL statement into a count statement
        /// </summary>
        /// <param name="sqlStatement">The SQL statement to be counted</param>
        /// <returns>The count statement</returns>
        public SqlStatement Count(SqlStatement sqlStatement)
        {
            return "SELECT COUNT(*) FROM (" + sqlStatement + ") Q0";
        }


        /// <summary>
        /// Create an EXISTS statement
        /// </summary>
        /// <param name="sqlStatement">The statement to determine EXISTS on</param>
        /// <returns>The constructed statement</returns>
        public SqlStatement Exists(SqlStatement sqlStatement)
        {
            return "SELECT CASE WHEN EXISTS (" + sqlStatement + ") THEN true ELSE false END FROM RDB$DATABASE";
        }

        /// <summary>
        /// Get reset sequence command
        /// </summary>
        public SqlStatement GetResetSequence(string sequenceName, object sequenceValue)
        {
            return new SqlStatement($"ALTER SEQUENCE {sequenceName} RESTART WITH {(int)sequenceValue}");
        }

        /// <inheritdoc/>
        public SqlStatement GetNextSequenceValue(String sequenceName) => new SqlStatement($"SELECT NEXT VALUE FOR {sequenceName} FROM RDB$DATABASE;')");

        /// <inheritdoc/>
        public SqlStatement CreateIndex(string indexName, string tableName, string column, bool isUnique)
        {
            return new SqlStatement($"CREATE {(isUnique ? "UNIQUE" : "")} INDEX {indexName} ON {tableName} ({column})");
        }

        /// <inheritdoc/>
        public SqlStatement DropIndex(string indexName)
        {
            return new SqlStatement($"DROP INDEX {indexName}");
        }


        /// <summary>
        /// Create SQL keyword
        /// </summary>
        /// <param name="keywordType">The type of keyword</param>
        /// <returns>The SQL equivalent</returns>
        public string CreateSqlKeyword(SqlKeyword keywordType)
        {
            switch (keywordType)
            {
                case SqlKeyword.ILike:
                case SqlKeyword.Like:
                    return " LIKE ";

                case SqlKeyword.Lower:
                    return " LOWER ";

                case SqlKeyword.Upper:
                    return " UPPER ";

                case SqlKeyword.False:
                    return " FALSE ";

                case SqlKeyword.True:
                    return " TRUE ";
                case SqlKeyword.CreateOrAlter:
                    return "CREATE OR ALTER ";
                case SqlKeyword.CreateMaterializedView:
                case SqlKeyword.CreateView:
                    return "CREATE OR ALTER VIEW ";
                case SqlKeyword.Union:
                    return " UNION ";
                case SqlKeyword.UnionAll:
                    return " UNION ALL ";
                case SqlKeyword.CurrentTimestamp:
                    return " CURRENT_TIMESTAMP ";
                case SqlKeyword.NewGuid:
                    return " GEN_UUID() ";
                default:
                    throw new ArgumentOutOfRangeException(nameof(keywordType));
            }
        }

        /// <inheritdoc/>
        public IEnumerable<IDbFilterFunction> GetFilterFunctions() => s_filterFunctions?.Values;
    }
}
