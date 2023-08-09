/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-5-19
 */
using SanteDB.Core;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.i18n;
using SanteDB.Core.Model.Map;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Configuration;
using SanteDB.OrmLite.Migration;
using SanteDB.OrmLite.Providers.Encryptors;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// Represents a IDbProvider for PostgreSQL
    /// </summary>
    [ExcludeFromCodeCoverage] // PostgreSQL is not used in unit testing
    public class PostgreSQLProvider : IDbMonitorProvider, IEncryptedDbProvider
    {
        // Last rr host used
#pragma warning disable CS0414 // The field 'PostgreSQLProvider.m_lastRrHost' is assigned but its value is never used
        private int m_lastRrHost = 0;
#pragma warning restore CS0414 // The field 'PostgreSQLProvider.m_lastRrHost' is assigned but its value is never used

        // Trace source
        private readonly Tracer m_tracer = new Tracer(Constants.TracerName + ".PostgreSQL");

        // DB provider factory
        private DbProviderFactory m_provider = null;

        // Index functions
#pragma warning disable CS0414 // The field 'PostgreSQLProvider.s_indexFunctions' is assigned but its value is never used
        private static Dictionary<String, IDbIndexFunction> s_indexFunctions = null;
#pragma warning restore CS0414 // The field 'PostgreSQLProvider.s_indexFunctions' is assigned but its value is never used
        // Monitor
        private IDiagnosticsProbe m_monitor;

        // Encryptor
        private IDbEncryptor m_encryptionProvider;
        private IOrmEncryptionSettings m_encryptionSettings;

        /// <summary>
        /// Invariant name
        /// </summary>
        public const string InvariantName = "npgsql";

        /// <summary>
        /// The factory this provider uses
        /// </summary>
        public const string ProviderFactoryType = "Npgsql.NpgsqlFactory, Npgsql";

        /// <inheritdoc/>
        public IDbStatementFactory StatementFactory { get; }

        /// <summary>
        /// Create new provider
        /// </summary>
        public PostgreSQLProvider()
        {
            this.StatementFactory = new PostgreSQLStatementFactory(this);
        }


        /// <summary>
        /// Trace SQL commands
        /// </summary>
        public bool TraceSql { get; set; }

        /// <summary>
        /// True if commands can be cancel
        /// </summary>
        public bool CanCancelCommands => true;

        /// <summary>
        /// Gets or sets the connection string
        /// </summary>
        public String ReadonlyConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the connection string
        /// </summary>
        public String ConnectionString { get; set; }

        /// <summary>
        /// Get name of provider
        /// </summary>
        public string Invariant => InvariantName;

        /// <summary>
        /// Get the monitor probe
        /// </summary>
        public IDiagnosticsProbe MonitorProbe
        {
            get
            {
                if (this.m_monitor == null)
                {
                    this.m_monitor = Diagnostics.OrmClientProbe.CreateProbe(this);
                }
                return this.m_monitor;
            }
        }

        /// <summary>
        /// Get provider factory
        /// </summary>
        /// <returns></returns>
        private DbProviderFactory GetProviderFactory()
        {
            if (this.m_provider == null) // HACK for Mono
            {
                var provType = ApplicationServiceContext.Current?.GetService<IConfigurationManager>().GetSection<OrmConfigurationSection>().AdoProvider.Find(o => o.Invariant.Equals(this.Invariant, StringComparison.OrdinalIgnoreCase))?.Type
                    ?? Type.GetType(ProviderFactoryType);
                if (provType == null)
                {
                    throw new InvalidOperationException("Cannot find NPGSQL provider");
                }

                this.m_provider = provType.GetField("Instance").GetValue(null) as DbProviderFactory;
            }
            if (this.m_provider == null)
            {
                throw new InvalidOperationException("Missing Npgsql provider");
            }

            return this.m_provider;
        }

        /// <summary>
        /// Gets a readonly connection
        /// </summary>
        public DataContext GetReadonlyConnection()
        {
            var conn = this.GetProviderFactory().CreateConnection();

            conn.ConnectionString = this.ReadonlyConnectionString ?? this.ConnectionString;
            return new DataContext(this, conn, true);
        }

        /// <summary>
        /// Get a connection that can be written to
        /// </summary>
        /// <returns></returns>
        public DataContext GetWriteConnection()
        {
            var conn = this.GetProviderFactory().CreateConnection();
            conn.ConnectionString = this.ConnectionString;
            return new DataContext(this, conn, false);
        }

        /// <summary>
        /// Create a command
        /// </summary>
        public IDbCommand CreateCommand(DataContext context, SqlStatement stmt)
        {
            var finStmt = stmt.Prepare();

#if DB_DEBUG
            if(System.Diagnostics.Debugger.IsAttached)
                this.Explain(context, CommandType.Text, finStmt.SQL, finStmt.Arguments.ToArray());
#endif

            return this.CreateCommandInternal(context, CommandType.Text, finStmt.Sql, finStmt.Arguments.ToArray());
        }


        // Parameter regex
        private static readonly Regex m_parmRegex = new Regex(@"\?", RegexOptions.Compiled);

        /// <summary>
        /// Create command internally
        /// </summary>
        private IDbCommand CreateCommandInternal(DataContext context, CommandType type, String sql, params object[] parms)
        {
            var pno = 0;

            sql = m_parmRegex.Replace(sql, o => $"@parm{pno++}");

            if (pno != parms.Length && type == CommandType.Text)
            {
                throw new ArgumentOutOfRangeException(nameof(sql), $"Parameter mismatch query expected {pno} but {parms.Length} supplied");
            }

            var cmd = context.Connection.CreateCommand();
            cmd.Transaction = context.Transaction;
            cmd.CommandType = type;
            cmd.CommandText = sql;

            if (this.TraceSql)
            {
                this.m_tracer.TraceEvent(EventLevel.Verbose, "[{0}] {1}", type, sql);
            }

            pno = 0;
            foreach (var itm in parms)
            {
                var parm = cmd.CreateParameter();
                var value = itm;

                // Parameter type
                parm.DbType = this.MapParameterType(value?.GetType());

                // Set value
                if (itm == null || itm == DBNull.Value)
                {
                    parm.Value = DBNull.Value;
                }
                else if (value?.GetType().IsEnum == true)
                {
                    parm.Value = (int)value;
                }
                else if (value is DateTimeOffset dto)
                {
                    parm.Value = dto.ToUniversalTime();
                }
                else if (value is DateTime dt)
                {
                    if (dt.Kind == DateTimeKind.Local)
                    {
                        parm.Value = dt.ToUniversalTime();
                    }
                    else if (dt.Kind == DateTimeKind.Unspecified)
                    {
                        parm.DbType = DbType.Date;
                        parm.Value = dt;
                    }
                    else
                    {
                        parm.Value = dt; // already utc
                    }
                }
                else
                {
                    parm.Value = itm;
                }

                if (type == CommandType.Text)
                {
                    parm.ParameterName = $"parm{pno++}";
                }

                parm.Direction = ParameterDirection.Input;

                if (this.TraceSql)
                {
                    this.m_tracer.TraceEvent(EventLevel.Verbose, "\t [{0}] {1} ({2})", cmd.Parameters.Count, parm.Value, parm.DbType);
                }

                cmd.Parameters.Add(parm);
            }


            return cmd;
        }

        /// <summary>
        /// Map a parameter type from the provided type
        /// </summary>
        public DbType MapParameterType(Type type)
        {
            // Null check
            if (type == null)
            {
                return DbType.Object;
            }

            switch (type.StripNullable().Name)
            {
                case nameof(DBNull):
                    return DbType.Object;

                case nameof(String):
                    return DbType.String;

                case nameof(DateTime):
                    return System.Data.DbType.DateTime;

                case nameof(DateTimeOffset):
                    return DbType.DateTimeOffset;

                case nameof(Int32):
                    return System.Data.DbType.Int32;

                case nameof(Int64):
                    return System.Data.DbType.Int64;

                case nameof(Boolean):
                    return System.Data.DbType.Boolean;

                case nameof(Single):
                case nameof(Double):
                    return System.Data.DbType.Double;

                case nameof(Decimal):
                    return DbType.Decimal;

                case nameof(Guid):
                    return DbType.Guid;

                case nameof(TimeSpan):
                    return DbType.Time;

                default:
                    if (type.StripNullable() == typeof(byte[]))
                    {
                        return System.Data.DbType.Binary;
                    }
                    else if (type.StripNullable().IsEnum)
                    {
                        return DbType.Int32;
                    }
                    else
                    {
                        throw new ArgumentOutOfRangeException(nameof(type), $"Can't map parameter type {type.Name}");
                    }
            }
        }

        /// <summary>
        /// Create a stored procedure command
        /// </summary>
        public IDbCommand CreateStoredProcedureCommand(DataContext context, string spName, params object[] parms)
        {
            return this.CreateCommandInternal(context, CommandType.StoredProcedure, spName, parms);
        }

        /// <summary>
        /// Create a command from string sql
        /// </summary>
        public IDbCommand CreateCommand(DataContext context, string sql, params object[] parms)
        {
            return this.CreateCommandInternal(context, CommandType.Text, sql, parms);
        }


        /// <inheritdoc/>
        public T ConvertValue<T>(object value) => (T)this.ConvertValue(value, typeof(T));

        /// <summary>
        /// Convert value just uses the mapper if needed
        /// </summary>
        public object ConvertValue(object value, Type toType)
        {
            object retVal = null;
            if (value != DBNull.Value && value != null)
            {
                if (toType.IsAssignableFrom(value.GetType()))
                {
                    return value;
                }
                else
                {
                    MapUtil.TryConvert(value, toType, out retVal);
                }
            }
            return retVal;
        }

        /// <summary>
        /// Create a new connection from an existing data source
        /// </summary>
        public DataContext CloneConnection(DataContext source)
        {
            var retVal = source.IsReadonly ? this.GetReadonlyConnection() : this.GetWriteConnection();
            retVal.ContextId = source.ContextId;
            return retVal;
        }



        /// <summary>
        /// Map datatype
        /// </summary>
        public string MapSchemaDataType(Type type)
        {
            type = type.StripNullable();
            if (type == typeof(byte[]))
            {
                return "BYTEA";
            }
            else
            {
                switch (type.Name)
                {
                    case nameof(Boolean):
                        return "BOOLEAN";
                    case nameof(DateTime):
                        return "DATE";
                    case nameof(DateTimeOffset):
                        return "TIMESTAMPTZ";
                    case nameof(Decimal):
                        return "DECIMAL";
                    case nameof(Single):
                    case nameof(Double):
                        return "FLOAT";
                    case nameof(Int32):
                        return "INTEGER";
                    case nameof(Int64):
                        return "BIGINT";
                    case nameof(String):
                        return "TEXT";
                    case nameof(Guid):
                        return "UUID";
                    default:
                        throw new NotSupportedException($"Schema type {type} not supported by PostgreSQL provider");
                }
            }
        }


        /// <summary>
        /// Get status of server connection
        /// </summary>
        public IEnumerable<DbStatementReport> StatActivity()
        {
            using (var conn = this.GetReadonlyConnection())
            {
                conn.Open();
                using (var cmd = conn.Connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "SELECT * FROM pg_stat_activity;";
                    using (var rdr = cmd.ExecuteReader())
                    {
                        while (rdr.Read())
                        {
                            yield return new DbStatementReport()
                            {
                                StatementId = rdr["pid"].ToString(),
                                Start = DateTime.Parse(rdr["query_start"].ToString()),
                                Status = rdr["state"].ToString() == "active" ? DbStatementStatus.Active : DbStatementStatus.Idle,
                                Query = rdr["query"].ToString()
                            };
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Get the name of the database
        /// </summary>
        public string GetDatabaseName()
        {
            var fact = this.GetProviderFactory().CreateConnectionStringBuilder();
            fact.ConnectionString = this.ConnectionString;
            fact.TryGetValue("database", out var value);
            return value?.ToString();
        }

        /// <inheritdoc/>
        private void InitializeApplicationEncryption()
        {

            // Is ALE even configured for this connection?
            if (this.m_encryptionSettings == null || this.m_encryptionProvider != null)
            {
                return;
            }

            // Attempt to connect to the PostgreSQL encryption provider to get the secret
            using (var connection = this.GetProviderFactory().CreateConnection())
            {
                try
                {
                    connection.ConnectionString = this.ReadonlyConnectionString;
                    connection.Open();
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = "SELECT TRUE FROM pg_proc WHERE proname ILIKE 'get_ale_smk'";
                        if (!this.ConvertValue<bool>(cmd.ExecuteScalar()))
                        {
                            return;
                        }

                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "get_ale_smk";
                        var aleSmk = this.ConvertValue<byte[]>(cmd.ExecuteScalar());


                        if (aleSmk != null)
                        {
                            if (this.m_encryptionSettings.Mode != OrmAleMode.Off)
                            {
                                this.m_encryptionProvider = new DefaultAesDataEncryptor(this.m_encryptionSettings, aleSmk);
                            }
                            else
                            {
                                cmd.CommandText = "DELETE FROM ale_smk;";
                                cmd.ExecuteNonQuery();
                                this.m_encryptionSettings.AleRecrypt(this);
                            }
                        }
                        else if (this.m_encryptionSettings.Mode != OrmAleMode.Off) // generate an ALE
                        {
                            this.m_tracer.TraceWarning("GENERATING AN APPLICATION LEVEL ENCRYPTION CERTIFICATE -> IT IS RECOMMENDED YOU USE TDE RATHER THAN ALE ON SANTEDB PRODUCTION INSTANCES");
                            aleSmk = DefaultAesDataEncryptor.GenerateMasterKey(this.m_encryptionSettings);
                            cmd.CommandText = "set_ale_smk";
                            var parm = cmd.CreateParameter();
                            parm.ParameterName = "NEW_ALE_SMK_IN";
                            parm.DbType = DbType.Binary;
                            parm.Direction = ParameterDirection.Input;
                            parm.Value = aleSmk;
                            cmd.Parameters.Add(parm);
                            cmd.ExecuteNonQuery();
                            this.m_encryptionSettings.AleRecrypt(this);
                            this.m_encryptionProvider = new DefaultAesDataEncryptor(this.m_encryptionSettings, aleSmk);
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Unable to load application layer encryption settings");
                    throw new DataException("Unable to load ALE encryption", e);
                }
            }
        }

        /// <summary>
        /// Get the encryption provider
        /// </summary>
        public IDbEncryptor GetEncryptionProvider()
        {
            this.InitializeApplicationEncryption();
            return this.m_encryptionProvider;
        }

        /// <summary>
        /// Set the encryption settings
        /// </summary>
        public void SetEncryptionSettings(IOrmEncryptionSettings ormEncryptionSettings)
        {
            if (this.m_encryptionSettings != null)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(SetEncryptionSettings)));
            }
            this.m_encryptionSettings = ormEncryptionSettings;
        }
    }
}