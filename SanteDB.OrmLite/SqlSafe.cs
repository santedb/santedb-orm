using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Static methods for SQL extensions
    /// </summary>
    internal static class SqlSafeExtensions
    {

        /// <summary>
        /// Sometimes we need to allow callers to reference tables directly inside of our SQL this allows us to sanitize them
        /// </summary>
        public static String Sanitize(this String me)
        {
            return me.Replace("'", "''");
        }
    }
}
