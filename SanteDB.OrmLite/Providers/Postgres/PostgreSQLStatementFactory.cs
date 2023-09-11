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
using SanteDB.Core;
using SanteDB.Core.Services;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// Statement factory for PostgreSQL
    /// </summary>
    internal class PostgreSQLStatementFactory : IDbStatementFactory
    {
        private static readonly ConcurrentDictionary<string, IDbFilterFunction> s_filterFunctions;

        /// <inheritdoc/>
        public String Invariant => PostgreSQLProvider.InvariantName;

        /// <summary>
        /// Create a new provider
        /// </summary>
        public PostgreSQLStatementFactory(PostgreSQLProvider provider)
        {
            this.Provider = provider;
        }

        /// <summary>
        /// Static CTOR
        /// </summary>
        static PostgreSQLStatementFactory()
        {
            if (ApplicationServiceContext.Current != null)
            {
                s_filterFunctions = new ConcurrentDictionary<string, IDbFilterFunction>(ApplicationServiceContext.Current.GetService<IServiceManager>()
                    .CreateInjectedOfAll<IDbFilterFunction>()
                    .Where(o => o.Provider == PostgreSQLProvider.InvariantName)
                    .ToDictionary(o => o.Name, o => o));
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
        /// SQL Engine features
        /// </summary>
        public SqlEngineFeatures Features
        {
            get
            {
                return SqlEngineFeatures.AutoGenerateGuids |
                    SqlEngineFeatures.AutoGenerateTimestamps |
                    SqlEngineFeatures.ReturnedInsertsAsReader |
                    SqlEngineFeatures.ReturnedUpdatesAsReader |
                    SqlEngineFeatures.StrictSubQueryColumnNames |
                    SqlEngineFeatures.LimitOffset |
                    SqlEngineFeatures.FetchOffset |
                    SqlEngineFeatures.MustNameSubQuery |
                    SqlEngineFeatures.AutoGenerateSequences |
                    SqlEngineFeatures.SetTimeout |
                    SqlEngineFeatures.MaterializedViews |
                    SqlEngineFeatures.Cascades |
                    SqlEngineFeatures.StoredFreetextIndex |
                    SqlEngineFeatures.Truncate;
            }
        }

        /// <summary>
        /// Return exists
        /// </summary>
        public SqlStatement Count(SqlStatement sqlStatement)
        {
            return "SELECT COUNT(*) FROM (" + sqlStatement + ") Q0";
        }

        /// <summary>
        /// Return exists
        /// </summary>
        public SqlStatement Exists(SqlStatement sqlStatement)
        {
            return "SELECT CASE WHEN EXISTS (" + sqlStatement + ") THEN true ELSE false END";
        }

        /// <summary>
        /// Append a returning statement
        /// </summary>
        public SqlStatement Returning(params ColumnMapping[] returnColumns)
        {
            if (returnColumns.Length == 0)
            {
                return SqlStatement.Empty;
            }

            return new SqlStatement($" RETURNING {String.Join(",", returnColumns.Select(o => o.Name))}");
        }

        /// <inheritdoc/>
        public SqlStatement GetNextSequenceValue(String sequenceName) => new SqlStatement($"SELECT nextval(?)", sequenceName);

        /// <summary>
        /// Create SQL keyword
        /// </summary>
        public string CreateSqlKeyword(SqlKeyword keywordType)
        {
            switch (keywordType)
            {
                case SqlKeyword.ILike:
                    return " ILIKE ";

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
                    return "CREATE OR REPLACE ";

                case SqlKeyword.RefreshMaterializedView:
                    return "REFRESH MATERIALIZED VIEW ";
                case SqlKeyword.CreateMaterializedView:
                    return "CREATE MATERIALIZED VIEW ";
                case SqlKeyword.CreateView:
                    return "CREATE OR REPLACE VIEW ";

                case SqlKeyword.Union:
                    return " UNION ";
                case SqlKeyword.UnionAll:
                    return " UNION ALL ";
                case SqlKeyword.Intersect:
                    return " INTERSECT ";
                case SqlKeyword.Vacuum:
                    return "VACUUM";
                case SqlKeyword.Analyze:
                    return "ANALYZE";
                case SqlKeyword.Reindex:
                    return "REINDEX SCHEMA public";
                case SqlKeyword.CurrentTimestamp:
                    return " CURRENT_TIMESTAMP ";
                case SqlKeyword.NewGuid:
                    return " UUID_GENERATE_V1() ";
                default:
                    throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Get reset sequence command
        /// </summary>
        public SqlStatement GetResetSequence(string sequenceName, object sequenceValue)
        {
            return new SqlStatement($"SELECT setval('{sequenceName}', {sequenceValue})");
        }

        /// <inheritdoc/>
        public SqlStatement CreateIndex(string indexName, string tableName, string column, bool isUnique)
        {
            return new SqlStatement($"CREATE {(isUnique ? "UNIQUE" : "")} INDEX {indexName} ON {tableName} USING BTREE ({column})");
        }

        /// <inheritdoc/>
        public SqlStatement DropIndex(string indexName)
        {
            return new SqlStatement($"DROP INDEX {indexName};");
        }

        /// <inheritdoc/>
        public IEnumerable<IDbFilterFunction> GetFilterFunctions() => s_filterFunctions?.Values;

        /// <summary>
        /// Get the provider
        /// </summary>
        public IDbProvider Provider { get; }
    }
}
