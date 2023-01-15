using SanteDB.Core;
using SanteDB.Core.Services;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// Statement factory for PostgreSQL
    /// </summary>
    public class PostgreSQLStatementFactory : IDbStatementFactory
    {
        private static readonly ConcurrentDictionary<string, IDbFilterFunction> s_filterFunctions;

        /// <inheritdoc/>
        public String Invariant => PostgreSQLProvider.InvariantName;

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
                    SqlEngineFeatures.MaterializedViews;
            }
        }

        /// <summary>
        /// Return exists
        /// </summary>
        public SqlStatement Count(SqlStatement sqlStatement)
        {
            return new SqlStatement(this, "SELECT COUNT(*) FROM (").Append(sqlStatement.Build()).Append(") Q0");
        }

        /// <summary>
        /// Return exists
        /// </summary>
        public SqlStatement Exists(SqlStatement sqlStatement)
        {
            return new SqlStatement(this, "SELECT CASE WHEN EXISTS (").Append(sqlStatement.Build()).Append(") THEN true ELSE false END");
        }

        /// <summary>
        /// Append a returning statement
        /// </summary>
        public SqlStatement Returning(SqlStatement sqlStatement, params ColumnMapping[] returnColumns)
        {
            if (returnColumns.Length == 0)
            {
                return sqlStatement;
            }

            return sqlStatement.Append($" RETURNING {String.Join(",", returnColumns.Select(o => o.Name))}");
        }

        /// <inheritdoc/>
        public SqlStatement GetNextSequenceValue(String sequenceName) => new SqlStatement(this, $"SELECT nextval(?)", sequenceName);

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
                default:
                    throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Get reset sequence command
        /// </summary>
        public SqlStatement GetResetSequence(string sequenceName, object sequenceValue)
        {
            return new SqlStatement(this, $"SELECT setval('{sequenceName}', {sequenceValue})");
        }

        /// <inheritdoc/>
        public SqlStatement CreateIndex(string indexName, string tableName, string column, bool isUnique)
        {
            return new SqlStatement(this, $"CREATE {(isUnique ? "UNIQUE" : "")} INDEX {indexName} ON {tableName} USING BTREE ({column})");
        }

        /// <inheritdoc/>
        public SqlStatement DropIndex(string indexName)
        {
            return new SqlStatement(this, $"DROP INDEX {indexName};");
        }

        /// <inheritdoc/>
        public IEnumerable<IDbFilterFunction> GetFilterFunctions() => s_filterFunctions?.Values;
    }
}
