using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents a database provider that can disable or enable constraints progammatically
    /// </summary>
    public interface IDisableConstraintProvider : IDbProvider
    {
        /// <summary>
        /// Disable all constraints on the connection
        /// </summary>
        /// <param name="context">The connection to disable constraint checks</param>
        void DisableAllConstraints(DataContext context);

        /// <summary>
        /// Enable all constraints on the connection
        /// </summary>
        /// <param name="context">The connection to enable constraint checks</param>
        void EnableAllConstraints(DataContext context);

    }
}
