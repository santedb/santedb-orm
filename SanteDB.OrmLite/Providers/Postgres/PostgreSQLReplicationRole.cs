using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// Replication mode
    /// </summary>
    internal enum PostgreSQLReplicationRole
    {
        origin,
        replica
    }
}
