using SanteDB.Core.i18n;
using SanteDB.Core.Model.Map;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Extensions for Orm
    /// </summary>
    public static class OrmExtensions
    {

        /// <summary>
        /// Load an extension using the extensions load extension method
        /// </summary>
        /// <param name="me">The connection on which the extension should be loaded</param>
        /// <param name="extensionName">The name of the extensions</param>
        public static void LoadExtension(this IDbConnection me, string extensionName)
        {
            var loadExtensionMethod = me.GetType().GetMethod("LoadExtension");
            if(loadExtensionMethod != null)
            {
                loadExtensionMethod.Invoke(me, new object[] { extensionName });
            }
        }

        /// <summary>
        /// Execute a scalar command returning the result
        /// </summary>
        public static TReturn ExecuteScalar<TReturn>(this IDbConnection me, String sql, params object[] parameters)
        {
            using(var cmd = me.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                int i = 0;
                foreach(var itm in parameters)
                {
                    if(itm is IDbDataParameter iddp)
                    {
                        cmd.Parameters.Add(iddp);
                    }
                    else
                    {
                        var parm = cmd.CreateParameter();
                        parm.ParameterName = $"@parm{i++}";
                        parm.Value = itm;
                        cmd.Parameters.Add(parm);
                    }

                }

                var retVal = cmd.ExecuteScalar();
                if(retVal is TReturn tr)
                {
                    return tr;
                }
                else if(MapUtil.TryConvert(retVal, typeof(TReturn), out var tr2))
                {
                    return (TReturn)tr2;
                }
                else
                {
                    throw new InvalidCastException(String.Format(ErrorMessages.MAP_INCOMPATIBLE_TYPE, retVal.GetType(), typeof(TReturn)));
                }
            }

        }
    }
}
