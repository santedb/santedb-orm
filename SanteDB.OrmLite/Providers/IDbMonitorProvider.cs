using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite.Providers
{

    /// <summary>
    /// Represents a monitoring DB provider
    /// </summary>
    public interface IDbMonitorProvider : IDbProvider
    {

        /// <summary>
        /// Stats the database to get running queries and state
        /// </summary>
        IEnumerable<DbStatementReport> StatActivity();

    }
}
