using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents a bulk data provider which can read/write from memory for faster inserts
    /// </summary>
    public interface IDbBulkProvider : IDbProvider
    {

        /// <summary>
        /// Gets a memory connection or temporary connection where data can be inserted quickly
        /// </summary>
        /// <returns>The memory data connection</returns>
        DataContext GetBulkConnection();

        /// <summary>
        /// Flushes the contents of <paramref name="bulkContext"/> into a connection obtained via <see cref="IDbProvider.GetWriteConnection"/>
        /// </summary>
        /// <param name="bulkContext">The data context opened by <see cref="GetBulkConnection"/> which is to be flushed</param>
        void FlushBulkConnection(DataContext bulkContext);

    }
}
