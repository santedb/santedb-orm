using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite
{

    /// <summary>
    /// Non-generic interface
    /// </summary>
    public interface IOrmResultSet : IEnumerable
    {

        /// <summary>
        /// Counts the number of records
        /// </summary>
        int Count();

        /// <summary>
        /// Skip N results
        /// </summary>
        IOrmResultSet Skip(int count);

        /// <summary>
        /// Take N results
        /// </summary>
        IOrmResultSet Take(int count);

        /// <summary>
        /// Gets the specified key
        /// </summary>
        IOrmResultSet Keys<TKey>();

        /// <summary>
        /// Convert this result set to an SQL statement
        /// </summary>
        SqlStatement ToSqlStatement();
    }
}
