using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Marks a provider which shields an underlying disk database with a memory writeback cache
    /// </summary>
    public interface IDbWriteBackProvider : IDbProvider
    {
        /// <summary>
        /// Get a persistent connection
        /// </summary>
        /// <returns>The persistent connection</returns>
        DataContext GetPersistentConnection();
    }
}
