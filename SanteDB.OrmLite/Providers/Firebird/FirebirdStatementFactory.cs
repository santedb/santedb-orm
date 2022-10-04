using SanteDB.Core;
using SanteDB.Core.Services;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
        /// <param name="sqlStatement">The SQL statement to "return"</param>
        /// <param name="returnColumns">The columns to return</param>
        /// <returns>The returned colums</returns>
        public SqlStatement Returning(SqlStatement sqlStatement, params ColumnMapping[] returnColumns)
        {
            if (returnColumns.Length == 0)
            {
                return sqlStatement;
            }

            return sqlStatement.Append($" RETURNING {String.Join(",", returnColumns.Select(o => o.Name))}");
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
            return new SqlStatement(this, "SELECT COUNT(*) FROM (").Append(sqlStatement.Build()).Append(") Q0");
        }


        /// <summary>
        /// Create an EXISTS statement
        /// </summary>
        /// <param name="sqlStatement">The statement to determine EXISTS on</param>
        /// <returns>The constructed statement</returns>
        public SqlStatement Exists(SqlStatement sqlStatement)
        {
            return new SqlStatement(this, "SELECT CASE WHEN EXISTS (").Append(sqlStatement.Build()).Append(") THEN true ELSE false END FROM RDB$DATABASE");
        }

        /// <summary>
        /// Get reset sequence command
        /// </summary>
        public SqlStatement GetResetSequence(string sequenceName, object sequenceValue)
        {
            return new SqlStatement(this, $"ALTER SEQUENCE {sequenceName} RESTART WITH {(int)sequenceValue}");
        }

        /// <inheritdoc/>
        public SqlStatement GetNextSequenceValue(String sequenceName) => new SqlStatement(this, $"SELECT NEXT VALUE FOR {sequenceName} FROM RDB$DATABASE;')");

        /// <inheritdoc/>
        public SqlStatement CreateIndex(string indexName, string tableName, string column, bool isUnique)
        {
            return new SqlStatement(this, $"CREATE {(isUnique ? "UNIQUE" : "")} INDEX {indexName} ON {tableName} ({column})");
        }

        /// <inheritdoc/>
        public SqlStatement DropIndex(string indexName)
        {
            return new SqlStatement(this, $"DROP INDEX {indexName}");
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
                default:
                    throw new ArgumentOutOfRangeException(nameof(keywordType));
            }
        }

    }
}
