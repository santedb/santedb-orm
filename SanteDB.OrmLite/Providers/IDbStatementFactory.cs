using System;
using System.Collections.Generic;
using System.Text;

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
        SqlStatement Returning(SqlStatement sqlStatement, params ColumnMapping[] returnColumns);

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

    }
}
