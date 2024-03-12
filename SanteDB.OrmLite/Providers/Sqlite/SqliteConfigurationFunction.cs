﻿/*
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
 * Date: 2023-11-30
 */
using SanteDB.Core.Diagnostics;
using System;
using System.Data;
using System.Reflection;
using System.Threading;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// Function to exectue configuration statements (PRAGMAs) on newly opened connections.
    /// </summary>
    public class SqliteConfigurationFunction : IDbInitializedFilterFunction
    {
        static int s_CheckedCompatability = 0;
        static Version s_SqliteVersion = null;
        static bool s_CanuseWal = false;

        readonly Tracer _Tracer = Tracer.GetTracer(typeof(SqliteConfigurationFunction));


        string IDbFilterFunction.Provider { get; } = SqliteProvider.InvariantName;

        string IDbFilterFunction.Name { get; } = "sqliteconfigurationfunction";

        SqlStatementBuilder IDbFilterFunction.CreateSqlStatement(SqlStatementBuilder currentBuilder, string filterColumn, string[] parms, string operand, Type operandType)
        {
            throw new NotSupportedException("This function should not be called with a statement. It is called automatically during connection initialization.");
        }

        bool IDbInitializedFilterFunction.Initialize(IDbConnection connection)
        {
            if (Interlocked.CompareExchange(ref s_CheckedCompatability, 1, 0) == 0)
            {
                var serverversionproperty = connection.GetType()?.GetRuntimeProperty("ServerVersion");

                string serverversion = null;

                if (null == serverversionproperty)
                {
                    serverversion = connection.ExecuteScalar<string>("SELECT sqlite_version() as version;");
                }
                else
                {
                    serverversion = serverversionproperty.GetValue(connection) as string;
                }

                if (!Version.TryParse(serverversion, out s_SqliteVersion))
                {
                    _Tracer.TraceWarning("Sqlite version reported \"{0}\" is not a valid version string. Certain optimizations may not work.", serverversion);
                }
                else
                {
                    _Tracer.TraceInfo("Sqlite version reported \"{0}\".", serverversion);

                    s_CanuseWal = s_SqliteVersion >= new Version(3, 7, 0);

                }
            }

            if (s_CanuseWal)
            {
                if ("wal".Equals(connection.ExecuteScalar<string>("PRAGMA journal_mode=wal;"), StringComparison.OrdinalIgnoreCase))
                {
                    //connection.Execute("PRAGMA synchronous=NORMAL;");
                }
                else
                {
                    _Tracer.TraceWarning("Sqlite attempted to set journal_mode=wal but did not get this journal mode back from the provider.");
                }



            }

            return true;
        }
    }
}
