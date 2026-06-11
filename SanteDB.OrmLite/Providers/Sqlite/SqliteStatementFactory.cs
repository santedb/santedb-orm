/*
 * Copyright (C) 2021 - 2026, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
using SanteDB.Core;
using SanteDB.Core.Services;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// Statement factory for SQLite
    /// </summary>
    public class SqliteStatementFactory : IDbStatementFactory
    {

        // Get next sequence lock
        private object m_getNextSequenceLock = new object();

        // Sequence lock value
        private uint m_sequenceLock = 0;

        // Offset of the sequence lock
        private uint m_offsetLock = 0;

        // Filter functions
        private static readonly ConcurrentDictionary<string, IDbFilterFunction> s_filterFunctions;

        /// <inheritdoc/>
        public String Invariant => SqliteProvider.InvariantName;

        /// <inheritdoc/>
        public IDbProvider Provider { get; }

        /// <summary>
        /// Static CTOR
        /// </summary>
        static SqliteStatementFactory()
        {
            
            if (ApplicationServiceContext.Current != null)
            {
                s_filterFunctions = new ConcurrentDictionary<string, IDbFilterFunction>(ApplicationServiceContext.Current.GetService<IServiceManager>()
                    .CreateInjectedOfAll<IDbFilterFunction>()
                    .Where(o => o.Provider == SqliteProvider.InvariantName)
                    .OrderBy(o => (o is IDbInitializedFilterFunction idiff) ? idiff.Order : 0)
                    .ToDictionary(o => o.Name, o => o));
            }

        }

        /// <summary>
        /// Create a new instance of the sqlite provider
        /// </summary>
        public SqliteStatementFactory(SqliteProvider sqliteProvider)
        {
            this.m_offsetLock = (uint)DateTimeOffset.Now.Subtract(new DateTime(2025, 01, 01)).TotalMinutes;
            this.Provider = sqliteProvider;
        }


        /// <summary>
        /// Gets the filter function
        /// </summary>
        public IDbFilterFunction GetFilterFunction(string name)
        {
            s_filterFunctions.TryGetValue(name, out var retVal);
            return retVal;
        }

        /// <inheritdoc/>
        public SqlEngineFeatures Features
        {
            get
            {
                return SqlEngineFeatures.AutoGenerateTimestamps |
                    SqlEngineFeatures.LimitOffset |
                    SqlEngineFeatures.ReturnedInsertsAsReader |
                    SqlEngineFeatures.ReturnedUpdatesAsReader |
                    SqlEngineFeatures.StrictSubQueryColumnNames |
                    SqlEngineFeatures.AutoGenerateGuids |
                    SqlEngineFeatures.AutoGeneratePrimaryKeySequences;
            }
        }

        /// <inheritdoc/>
        public SqlStatement Count(SqlStatement sqlStatement)
        {
            return new SqlStatement("SELECT COUNT(*) FROM (").Append(sqlStatement).Append(") Q0");
        }

        /// <inheritdoc/>
        public SqlStatement Exists(SqlStatement sqlStatement)
        {
            return new SqlStatement("SELECT CASE WHEN EXISTS (").Append(sqlStatement).Append(") THEN true ELSE false END");
        }

        /// <inheritdoc/>
        public SqlStatement Returning(params ColumnMapping[] returnColumns)
        {
            if (returnColumns.Length == 0)
            {
                return SqlStatement.Empty;
            }

            return new SqlStatement($" RETURNING {String.Join(",", returnColumns.Select(o => o.Name))}");
        }

        /// <inheritdoc/>
        public string CreateSqlKeyword(SqlKeyword keywordType)
        {
            switch (keywordType)
            {
                case SqlKeyword.False:
                    return "0";
                case SqlKeyword.True:
                    return "1";
                case SqlKeyword.ILike:
                case SqlKeyword.Like:
                    return " LIKE ";

                case SqlKeyword.Lower:
                    return " LOWER ";

                case SqlKeyword.Upper:
                    return " UPPER ";

                case SqlKeyword.CreateView:
                case SqlKeyword.CreateMaterializedView:
                    return "CREATE VIEW IF NOT EXISTS ";
                case SqlKeyword.Union:
                case SqlKeyword.UnionAll:
                    return " UNION ";
                case SqlKeyword.Intersect:
                    return " INTERSECT ";
                case SqlKeyword.Vacuum:
                    return "VACUUM";
                case SqlKeyword.Reindex:
                    return "REINDEX";
                case SqlKeyword.Analyze:
                    return "ANALYZE";
                case SqlKeyword.CurrentTimestamp:
                    return " (unixepoch()) ";
                case SqlKeyword.NewGuid:
                    return " (randomblob(16)) ";
                case SqlKeyword.DeferConstraints:
                    return "DEFERRABLE INITIALLY DEFERRED";
                case SqlKeyword.StringAggregate:
                    return " GROUP_CONCAT";
                default:
                    throw new NotImplementedException();
            }
        }

        /// <inheritdoc/>
        public SqlStatement GetResetSequence(string sequenceName, object sequenceValue)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public SqlStatement CreateIndex(string indexName, string tableName, string column, bool isUnique)
        {
            return new SqlStatement($"CREATE {(isUnique ? "UNIQUE" : "")} INDEX {indexName.Sanitize()} ON {tableName.Sanitize()} ({column.Sanitize()})");
        }

        /// <inheritdoc/>
        public SqlStatement DropIndex(string indexName)
        {
            return new SqlStatement($"DROP INDEX {indexName}");
        }

        /// <inheritdoc/>
        public SqlStatement GetNextSequenceValue(string sequenceName)
        {
            // Move the "minutes" from epoch up to the upper half of a long and then placethe current sequence lock into the lower 16 bits
            // JF: This algorithm has changed to deal with JavaScript maxing out at 53 bit whole numbers - the new algorithm is
            // 0000 0000 EEEE EEEE EEEE EEEE EEEE EEEE EEEE EEEE CCCC CCCC CCCC CCCC CCCC CCCC
            lock (this.m_getNextSequenceLock)
            {
                var increment = this.m_sequenceLock++;
                if (increment == 0xFF_FFFF) // 24-bit number maximum -> we jump our total minutes since start 
                {
                    this.m_offsetLock = (uint)DateTimeOffset.Now.Subtract(new DateTime(2025, 01, 01)).TotalMinutes;
                    this.m_sequenceLock = 0;
                }
                return new SqlStatement((((ulong)m_offsetLock << 24) | (ulong)increment).ToString());  // new SqlStatement($"SELECT COALESCE(MAX(ROWID), 0) + 1 FROM {sequenceName.Sanitize()}");
            }
        }

        /// <inheritdoc/>
        public IEnumerable<IDbFilterFunction> GetFilterFunctions() => s_filterFunctions?.Values;
    }
}
