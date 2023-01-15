using SanteDB.Core;
using SanteDB.Core.Services;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// Statement factory for SQLite
    /// </summary>
    public class SqliteStatementFactory : IDbStatementFactory
    {

        // Filter functions
        private static readonly ConcurrentDictionary<string, IDbFilterFunction> s_filterFunctions;

        /// <inheritdoc/>
        public String Invariant => SqliteProvider.InvariantName;

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

        /// <inheritdoc/>
        public SqlEngineFeatures Features
        {
            get
            {
                return SqlEngineFeatures.AutoGenerateTimestamps | SqlEngineFeatures.LimitOffset | SqlEngineFeatures.AutoGenerateGuids | SqlEngineFeatures.ReturnedInsertsAsReader | SqlEngineFeatures.ReturnedUpdatesAsReader | SqlEngineFeatures.StrictSubQueryColumnNames;
            }
        }

        /// <inheritdoc/>
        public SqlStatement Count(SqlStatement sqlStatement)
        {
            return new SqlStatement(this, "SELECT COUNT(*) FROM (").Append(sqlStatement.Build()).Append(") Q0");
        }

        /// <inheritdoc/>
        public SqlStatement Exists(SqlStatement sqlStatement)
        {
            return new SqlStatement(this, "SELECT CASE WHEN EXISTS (").Append(sqlStatement.Build()).Append(") THEN true ELSE false END");
        }

        /// <inheritdoc/>
        public SqlStatement Returning(SqlStatement sqlStatement, params ColumnMapping[] returnColumns)
        {
            if (returnColumns.Length == 0)
            {
                return sqlStatement;
            }

            return sqlStatement.Append($" RETURNING {String.Join(",", returnColumns.Select(o => o.Name))}");
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
            return new SqlStatement(this, $"CREATE {(isUnique ? "UNIQUE" : "")} INDEX {indexName.Sanitize()} ON {tableName.Sanitize()} ({column.Sanitize()})");
        }

        /// <inheritdoc/>
        public SqlStatement DropIndex(string indexName)
        {
            return new SqlStatement(this, $"DROP INDEX {indexName}");
        }

        /// <inheritdoc/>
        public SqlStatement GetNextSequenceValue(string sequenceName)
        {
            return new SqlStatement(this, $"SELECT MAX(ROWID) + 1 FROM {sequenceName.Sanitize()}");
        }

        /// <inheritdoc/>
        public IEnumerable<IDbFilterFunction> GetFilterFunctions() => s_filterFunctions?.Values;
    }
}
