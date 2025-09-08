/*
 * Copyright (C) 2021 - 2025, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
using SanteDB.Core;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.i18n;
using SanteDB.Core.Model.Map;
using SanteDB.Core.Security;
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
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// Represents a IDbProvider for PostgreSQL
    /// </summary>
    [ExcludeFromCodeCoverage] // PostgreSQL is not used in unit testing
    public class PostgreSQLProvider : IDbMonitorProvider, IEncryptedDbProvider, IReportProgressChanged, IDisableConstraintProvider, IDbBackupProvider
    {
        /// <summary>
        /// The number of tries to restore a table before the operation fails. The upper limit should 
        /// be the number of tables since it's possible (though extremely unlikely) a table could depend 
        /// on every other table being restored in the database however it is first in order for restore. 
        /// </summary>
        private const int MAX_RESTORE_TRIES = 100;

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
        /// Delegates to specific methods of the npgsql provider which we cannot directly access because we don't take a dependency on npgsql.
        /// </summary>
        private PostgreProviderExtendedFunctions m_ProviderFunctions;

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

        /// <inheritdoc/>
        public event EventHandler<ProgressChangedEventArgs> ProgressChanged;

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
                        if (dt.TimeOfDay.Hours == 0 && dt.TimeOfDay.Minutes == 0 && dt.TimeOfDay.Seconds == 0 && dt.TimeOfDay.Milliseconds == 0) // A date expressed
                        {
                            parm.DbType = DbType.Date;
                            parm.Value = dt;
                        }
                        else
                        {
                            parm.Value = dt.ToUniversalTime();
                        }
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
            var retVal = this.GetReadonlyConnection();
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
                conn.Open(initializeExtensions: false);
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
                    connection.ConnectionString = this.ConnectionString;
                    connection.Open();
                    byte[] aleSmk = null;
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = "SELECT TRUE FROM pg_proc WHERE proname ILIKE 'get_ale_smk'";
                        if (this.ConvertValue<bool?>(cmd.ExecuteScalar()) != true)
                        {
                            return;
                        }

                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = "select get_ale_smk()";
                        aleSmk = this.ConvertValue<byte[]>(cmd.ExecuteScalar());
                    }

                    if (aleSmk != null) // SMK already set
                    {
                        this.m_encryptionProvider = new DefaultAesDataEncryptor(this.m_encryptionSettings, aleSmk);
                    }
                    else if (this.m_encryptionSettings.AleEnabled) // generate an ALE
                    {
                        using (AuthenticationContext.EnterSystemContext())
                        {
                            this.MigrateEncryption(this.m_encryptionSettings);
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
            else if (ormEncryptionSettings.AleEnabled)
            {
                this.m_encryptionSettings = ormEncryptionSettings;
            }
        }

        /// <inheritdoc/>
        public void MigrateEncryption(IOrmEncryptionSettings newOrmEncryptionSettings)
        {

            // Is ALE even configured for this connection?
            if (this.m_encryptionSettings == null && newOrmEncryptionSettings.AleEnabled == false)
            {
                return;
            }
            if (!(this.m_encryptionSettings is OrmAleConfiguration aleConfiguration) ||
                AuthenticationContext.Current.Principal != AuthenticationContext.SystemPrincipal)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(MigrateEncryption)));
            }

            // Decrypt the databases
            // Attempt to connect to the PostgreSQL encryption provider to get the secret
            using (var connection = this.GetProviderFactory().CreateConnection())
            {
                try
                {
                    connection.ConnectionString = this.ConnectionString;
                    connection.Open();

                    using (var cmd = connection.CreateCommand())
                    {

                        if (this.m_encryptionProvider != null) // current encryption provider so decrypt
                        {
                            this.m_tracer.TraceInfo("Decrypting with old key...");
                            aleConfiguration.DisableAll();
                            this.m_encryptionSettings.AleRecrypt(this);
                            this.m_encryptionSettings = null;
                            this.m_encryptionProvider = null;
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = "DELETE FROM ale_systbl;";
                            cmd.ExecuteNonQuery();
                        }

                        this.m_tracer.TraceInfo("Encrypting with new key...");
                        if (newOrmEncryptionSettings.AleEnabled)
                        {
                            this.m_tracer.TraceWarning("GENERATING AN APPLICATION LEVEL ENCRYPTION CERTIFICATE -> IT IS RECOMMENDED YOU USE TDE RATHER THAN ALE ON SANTEDB PRODUCTION INSTANCES");
                            var aleSmk = DefaultAesDataEncryptor.GenerateMasterKey(newOrmEncryptionSettings);
                            cmd.CommandText = "select set_ale_smk(@NEW_ALE_SMK_IN)";
                            var parm = cmd.CreateParameter();
                            parm.ParameterName = "NEW_ALE_SMK_IN";
                            parm.DbType = DbType.Binary;
                            parm.Direction = ParameterDirection.Input;
                            parm.Value = aleSmk;
                            cmd.Parameters.Add(parm);
                            cmd.ExecuteNonQuery();
                            this.m_encryptionSettings = newOrmEncryptionSettings;
                            this.m_encryptionSettings.AleRecrypt(this, (a, b, c) => this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs(a, b, c)));
                            this.m_encryptionProvider = new DefaultAesDataEncryptor(newOrmEncryptionSettings, aleSmk);
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Unable to migrate application layer encryption settings");
                    throw new DataException("Unable to migrate ALE encryption", e);
                }
            }
        }

        /// <summary>
        /// Optimize the database
        /// </summary>
        public void Optimize()
        {
            using (var writer = this.GetWriteConnection())
            {
                writer.Open(initializeExtensions: false);
                writer.ExecuteNonQuery(this.StatementFactory.CreateSqlKeyword(SqlKeyword.Vacuum));
                writer.ExecuteNonQuery(this.StatementFactory.CreateSqlKeyword(SqlKeyword.Reindex));
                writer.ExecuteNonQuery(this.StatementFactory.CreateSqlKeyword(SqlKeyword.Analyze));
            }
        }

        /// <inheritdoc/>
        public void DisableAllConstraints(DataContext context)
        {
            // Only the dCDR gateway is allowed to do this
            if (ApplicationServiceContext.Current.HostType != SanteDBHostType.Gateway)
            {
                return;
            }

            context.ExecuteNonQuery("SET CONSTRAINTS ALL DEFERRED");
            // Get all tables
            foreach (var tbl in context.ExecQuery<String>(new SqlStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type <> 'VIEW'")).ToArray())
            {
                context.ExecuteNonQuery($"ALTER TABLE {tbl} DISABLE TRIGGER ALL");
            }
        }

        /// <inheritdoc/>
        public void EnableAllConstraints(DataContext context)
        {
            // Only the dCDR gateway is allowed to do this
            if (ApplicationServiceContext.Current.HostType != SanteDBHostType.Gateway)
            {
                return;
            }

            // Get all tables
            foreach (var tbl in context.ExecQuery<String>(new SqlStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type <> 'VIEW'")).ToArray())
            {
                context.ExecuteNonQuery($"ALTER TABLE {tbl} ENABLE TRIGGER ALL");
            }
        }

        /// <inheritdoc/>
        public void InitializeConnection(IDbConnection connection)
        {

        }

        /// <inheritdoc />
        public bool BackupToStream(System.IO.Stream backupStream)
        {
            m_tracer.TraceInfo("Backing up database to stream.");
            m_tracer.TraceUntestedWarning();

            using (var ctx = this.GetWriteConnection())
            {
                ctx.Open();

                if (null == m_ProviderFunctions)
                    m_ProviderFunctions = new PostgreProviderExtendedFunctions(ctx.Connection.GetType());

                if (null == m_ProviderFunctions || !m_ProviderFunctions.IsSupported)
                {
                    m_tracer.TraceWarning("Backup feature is not supported by the provider. Ensure you are using the latest version of npgsql.");
                    return false;
                }

                var pgversion = m_ProviderFunctions.PostgreSqlVersion(ctx.Connection);

                if (null == pgversion)
                {
                    m_tracer.TraceWarning("Backup feature is not supported by the provider. The server version cannot be determined.");
                    return false;
                }

                if (pgversion.Major < 15)
                {
                    m_tracer.TraceWarning("Backup feature is not supported by the PostgreSQL server. PostgreSQL version 15 or later is required to use this feature.");
                    return false;
                }

                return BackupInternal(backupStream, ctx);
            }
        }

        /// <inheritdoc />
        public bool RestoreFromStream(System.IO.Stream restoreStream)
        {
            m_tracer.TraceInfo("Restoring database from stream.");
            m_tracer.TraceUntestedWarning();

            using (var ctx = this.GetWriteConnection())
            {
                ctx.Open();

                if (null == m_ProviderFunctions)
                    m_ProviderFunctions = new PostgreProviderExtendedFunctions(ctx.Connection.GetType());

                if (null == m_ProviderFunctions || !m_ProviderFunctions.IsSupported)
                {
                    m_tracer.TraceError("Restore from backup feature is not supported by the provider. Ensure you are using the latest version of npgsql.");
                    return false;
                }

                var pgversion = m_ProviderFunctions.PostgreSqlVersion(ctx.Connection);

                if (null == pgversion)
                {
                    m_tracer.TraceError("Restore from backup feature is not supported by the provider. The server version cannot be determined.");
                    return false;
                }

                if (pgversion.Major < 15)
                {
                    m_tracer.TraceError("Restore from backup feature is not supported by the server. PostgreSQL version 15 or later is required to use this feature.");
                    return false;
                }

                return RestoreInternal(restoreStream, ctx);
            }
        }

        private bool BackupInternal(System.IO.Stream destinationStream, DataContext ctx)
        {
            var tablestobackup = GetBackupRestoreTableNames(ctx);

            var originalrole = GetSessionReplicationRole(ctx);

            m_tracer.TraceInfo("Beginning backup operation.");

            try
            {
                using (var tarwriter = new SharpCompress.Writers.Tar.TarWriter(
                    destinationStream,
                    new SharpCompress.Writers.Tar.TarWriterOptions(
                        SharpCompress.Common.CompressionType.None,
                        true)))
                {
                    foreach (var table in tablestobackup)
                    {
                        var tempfilename = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"santedb_backuptemp_{table}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}");

                        try
                        {
                            using (var tmpfs = new System.IO.FileStream(tempfilename, System.IO.FileMode.Create, System.IO.FileAccess.ReadWrite))
                            {
                                using (var rdr = BeginCopyOut(ctx, table))
                                {
                                    rdr.CopyTo(tmpfs);

                                    rdr.Close();

                                    tmpfs.Seek(0, System.IO.SeekOrigin.Begin);

                                    tarwriter.Write($"tables/{table}.bin", tmpfs, DateTime.Now);
                                }
                            }
                        }
                        finally
                        {
                            System.IO.File.Delete(tempfilename);
                        }

                        m_tracer.TraceInfo("Backed up table '{0}'", table);
                    }
                }

                destinationStream.Flush();

                m_tracer.TraceInfo("Ending backup operation successfully.");

                return true;
            }
            finally
            {
                try
                {
                    SetSessionReplicationRole(ctx, originalrole);
                }
                catch { }
            }
        }

        private bool TruncateTablesInternal(DataContext ctx, List<string> tablesToTruncate)
        {
            var originalrole = GetSessionReplicationRole(ctx);
            SetSessionReplicationRole(ctx, "replica");

            m_tracer.TraceInfo("Truncate Tables: beginning transaction.");
            using (var txn = ctx.BeginTransaction())
                try
                {

                    foreach (var table in tablesToTruncate)
                    {
                        m_tracer.TraceInfo("Start Truncate Table {0}", table);
                        ctx.ExecuteNonQuery($"TRUNCATE TABLE \"public\".\"{table}\" CASCADE;");
                        m_tracer.TraceInfo("End Truncate Table {0}", table);
                    }

                    m_tracer.TraceInfo("Truncate Tables: committing transaction.");
                    txn.Commit();

                    return true;
                }
                catch (DbException dbex)
                {
                    try
                    {
                        m_tracer.TraceError("Error during truncate tables. Attempting rollback. This operation could take a significant amount of time. Exception: {0}", dbex.ToHumanReadableString());
                        txn?.Rollback();
                    }
                    catch (DbException dbex2)
                    {
                        throw new DataException($"Error during rollback of truncate tables. See inner exception for details. This connection must be closed.\r\nOriginal Exception: {dbex.ToHumanReadableString()}", dbex2);
                    }

                    throw new DataException("Failed to truncate tables during restore operation. See inner exception for details.", dbex);
                }
                finally
                {
                    try
                    {
                        SetSessionReplicationRole(ctx, originalrole);
                    }
                    catch { }
                }

        }



        private bool RestoreInternal(System.IO.Stream sourceStream, DataContext ctx)
        {
            var tablestorestore = GetBackupRestoreTableNames(ctx);

            var tmpfolder = System.IO.Path.GetTempPath();

            if (!TruncateTablesInternal(ctx, tablestorestore))
                return false;

            var originalrole = GetSessionReplicationRole(ctx);
            
            using (var txn = ctx.BeginTransaction())
                try
                {
                    SetSessionReplicationRole(ctx, "replica");
                    ctx.ExecuteNonQuery("SET constraints ALL DEFERRED;");

                    var deferredtables = new Queue<System.ValueTuple<string, string, int>>();

                    //Primary restore - copies the tables to temp files and restores immediately.
                    using(var tar = SharpCompress.Readers.Tar.TarReader.Open(sourceStream))
                    {
                        while (tar.MoveToNextEntry())
                        {
                            var entry = tar.Entry;
                            var tablename = System.IO.Path.GetFileNameWithoutExtension(entry.Key);

                            if (null == tablename)
                                continue;

                            if (tablestorestore.Contains(tablename))
                            {
                                m_tracer.TraceInfo("Beginning Restore for table {0}", tablename);

                                m_ProviderFunctions.TransactionSavepointSave(txn, tablename);

                                var tmpfilename = System.IO.Path.Combine(tmpfolder, $"santedbrestore_{tablename}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}.bin");
                                var deletefile = true;

                                using (var tmpfs = new System.IO.FileStream(tmpfilename, System.IO.FileMode.Create, System.IO.FileAccess.ReadWrite))
                                {
                                    using (var entrystream = tar.OpenEntryStream())
                                    {
                                        entrystream.CopyTo(tmpfs);
                                    }

                                    tmpfs.Seek(0, System.IO.SeekOrigin.Begin);

                                    try
                                    {
                                        using (var wrtr = BeginCopyIn(ctx, tablename))
                                        {
                                            tmpfs.CopyTo(wrtr);
                                            wrtr.Close();
                                        }
                                        m_tracer.TraceInfo("Restored table {0}", tablename);
                                    }
                                    catch (DbException)
                                    {
                                        m_tracer.TraceInfo("Deferring table {0}", tablename);

                                        m_ProviderFunctions.TransactionSavepointRollback(txn, tablename);

                                        deferredtables.Enqueue(new System.ValueTuple<string, string, int>(tablename, tmpfilename, 1));
                                        deletefile = false;
                                    }
                                }

                                if (deletefile)
                                    System.IO.File.Delete(tmpfilename);

                            }
                            else
                            {
                                m_tracer.TraceInfo("Found entry for table {0} which does not exist or is unsuitable for restore in the database.", tablename);

                                //Fast-forward through the entry.
                                using(var tarstream = tar.OpenEntryStream())
                                {
                                    tarstream.SkipEntry();
                                    tarstream.Close();
                                }
                            }
                        }
                    }

                    while(deferredtables.Count > 0)
                    {
                        var entry = deferredtables.Dequeue();
                        var tablename = entry.Item1;
                        var tmpfilename = entry.Item2;
                        var tries = entry.Item3;
                        var deletefile = true;

                        m_ProviderFunctions.TransactionSavepointSave(txn, tablename);

                        using (var tmpfs = new System.IO.FileStream(tmpfilename, System.IO.FileMode.Open, System.IO.FileAccess.ReadWrite))
                        {
                            try
                            {
                                using (var wrtr = BeginCopyIn(ctx, tablename))
                                {
                                    tmpfs.CopyTo(wrtr);
                                    wrtr.Close();

                                    m_tracer.TraceInfo("Restored table {0}", tablename);
                                }
                            }
                            catch (DbException dbex)
                            {
                                if (tries > MAX_RESTORE_TRIES)
                                {
                                    m_tracer.TraceError("Restore failed. Maximum retries exceeded for table {0}", tablename);
                                    m_tracer.TraceInfo("Rolling back restore transaction. This operation could take a significant amount of time.");
                                    txn.Rollback();
                                    throw new DataException("Exception during restore operation. See inner exception for details. The transaction has been rolled back.", dbex);
                                }

                                m_tracer.TraceInfo("Deferring table {0}", tablename);

                                m_ProviderFunctions.TransactionSavepointRollback(txn, tablename);

                                deferredtables.Enqueue(new System.ValueTuple<string, string, int>(tablename, tmpfilename, tries + 1));
                                deletefile = false;
                            }
                        }

                        if (deletefile)
                            System.IO.File.Delete(tmpfilename);

                    }
                }
                catch (DbException dbex)
                {
                    try
                    {
                        m_tracer.TraceError("Error during table restore operation. Attempting rollback. This operation could take a significant amount of time. Exception {0}", dbex.ToHumanReadableString());
                        txn.Rollback();
                    }
                    catch (DbException dbex2)
                    {
                        throw new DataException($"Error during rollback of truncate tables. See inner exception for details. This connection must be closed.\r\nOriginal Exception: {dbex.ToHumanReadableString()}", dbex2);
                    }

                    throw new DataException("Unexpected failure during table restore. See inner exception for details.", dbex);
                }

            return true;
        }

        private System.IO.Stream BeginCopyIn(DataContext ctx, string tableName)
            => m_ProviderFunctions.BeginRawBinaryCopy(ctx.Connection, $"COPY \"public\".\"{tableName}\" FROM STDIN (FORMAT BINARY);");

        private System.IO.Stream BeginCopyOut(DataContext ctx, string tableName)
            => m_ProviderFunctions.BeginRawBinaryCopy(ctx.Connection, $"COPY \"public\".\"{tableName}\" TO STDOUT (FORMAT BINARY);");

        private static string GetSessionReplicationRole(DataContext ctx)
            => ctx.Query<string>(new SqlStatement("SHOW session_replication_role;"))?.FirstOrDefault();

        private static void SetSessionReplicationRole(DataContext ctx, string role)
            => ctx.ExecuteNonQuery("SET session_replication_role = ?;", role);

        /// <summary>
        /// Retrieves a list of tables which are probably suitable for backup and restore in the database.
        /// </summary>
        /// <param name="ctx">The data context to use to retrieve the table names from.</param>
        /// <returns>A list of bare table names. These tables are in the public schema.</returns>
        private static List<string> GetBackupRestoreTableNames(DataContext ctx)
        {
            return ctx.Query<string>(new SqlStatement("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema = 'public';"))?.ToList() ?? new List<string>();
        }
    }
}