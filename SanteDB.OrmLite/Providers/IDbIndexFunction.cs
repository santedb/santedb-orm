using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// A function which can translate codes to indexing 
    /// </summary>
    public interface IDbIndexFunction
    {

        /// <summary>
        /// Gets the name of the index
        /// </summary>
        String Name { get; }

        /// <summary>
        /// Gets the provider to which this index function applies
        /// </summary>
        String Provider { get; }

        /// <summary>
        /// Create the statement to define the index
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="column">The column to be indexed</param>
        /// <param name="tableName">The table to be indexed</param>
        SqlStatement CreateIndex(String indexName, String tableName, String column);


    }
}
