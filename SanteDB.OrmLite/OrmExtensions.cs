/*
 * Copyright (C) 2021 - 2024, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
 * Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors
 * Portions Copyright (C) 2015-2018 Mohawk College of Applied Arts and Technology
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. You may 
 * obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations under 
 * the License.
 * 
 * User: fyfej
 * Date: 2023-6-21
 */
using SanteDB.Core.i18n;
using SanteDB.Core.Model.Map;
using System;
using System.Data;

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
            if (loadExtensionMethod != null)
            {
                loadExtensionMethod.Invoke(me, new object[] { extensionName });
            }
        }

        /// <summary>
        /// Execute a command that does not return a value.
        /// </summary>
        /// <param name="me">The connection to execute the command on.</param>
        /// <param name="sql">The statement to run in the command.</param>
        /// <param name="parameters">Parameters for the statement that will be inserted into the statement.</param>
        public static void Execute(this IDbConnection me, String sql, params object[] parameters)
        {
            using (var cmd = me.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                int i = 0;
                foreach (var itm in parameters)
                {
                    if (itm is IDbDataParameter iddp)
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

                var retVal = cmd.ExecuteNonQuery();

            }
        }

        /// <summary>
        /// Execute a scalar command returning the result
        /// </summary>
        public static TReturn ExecuteScalar<TReturn>(this IDbConnection me, String sql, params object[] parameters)
        {
            using (var cmd = me.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                int i = 0;
                foreach (var itm in parameters)
                {
                    if (itm is IDbDataParameter iddp)
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
                if(retVal == DBNull.Value || retVal == null)
                {
                    return default(TReturn);
                }
                else if (retVal is TReturn tr)
                {
                    return tr;
                }
                else if (MapUtil.TryConvert(retVal, typeof(TReturn), out var tr2))
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
