﻿/*
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
using DocumentFormat.OpenXml.Bibliography;
using DocumentFormat.OpenXml.Office.CustomUI;
using SanteDB;
using SanteDB.Core;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.i18n;
using SanteDB.Core.Model.Map;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Configuration;
using SanteDB.OrmLite.Migration;
using SanteDB.OrmLite.Providers.Encryptors;
using SharpCompress;
using SQLitePCL;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Tracing;
using System.IO;
using System.IO.IsolatedStorage;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// SQL Lite provider
    /// </summary>
    public class SqliteProvider : IDbProvider, IEncryptedDbProvider, IReportProgressChanged, IDbBackupProvider, IDbMonitorProvider //, IDbBulkProvider
    {


        /// <summary>
        /// Invariant name
        /// </summary>
        public const string InvariantName = "sqlite";

        /// <summary>
        /// The factory this provider uses
        /// </summary>
        public const string ProviderFactoryType = "Microsoft.Data.Sqlite.SqliteFactory, Microsoft.Data.Sqlite";

        // UUID regex
        private static readonly Regex m_uuidRegex = new Regex(@"(\'[A-Za-z0-9]{8}\-[A-Za-z0-9]{4}\-[A-Za-z0-9]{4}\-[A-Za-z0-9]{4}\-[A-Za-z0-9]{12}\')", RegexOptions.Compiled);
        private static readonly Regex m_parmRegex = new Regex(@"\?", RegexOptions.Compiled);
        private static readonly Regex m_limitOffsetRegex = new Regex(@"(OFFSET\s\d+)\s+(LIMIT\s\d+)?", RegexOptions.Compiled);
        private static readonly Regex m_dateRegex = new Regex(@"(CURRENT_TIMESTAMP|CURRENT_DATE)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        // Bulk Initialization commands
        private readonly object m_lock = new object();
        private String[] m_seedDatabaseCommands = null;

        // Provider
        private DbProviderFactory m_provider = null;

        // Blocker for backup process
        protected readonly ManualResetEventSlim m_lockoutEvent = new ManualResetEventSlim(true);

        // Monitoring probe
        private IDiagnosticsProbe m_monitor;

        // Tracer for the objects
        private readonly Tracer m_tracer = new Tracer(Constants.TracerName + ".Sqlite");

        // Database certificate
        private IOrmEncryptionSettings m_encryptionSettings;

        private DefaultAesDataEncryptor m_encryptionProvider;

        /// <inheritdoc/>
        public event EventHandler<ProgressChangedEventArgs> ProgressChanged;

        /// <summary>
        /// Gets or sets the connection string for this provier
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the readonly connection string
        /// </summary>
        public string ReadonlyConnectionString { get; set; }

        /// <summary>
        /// True if commands can be cancel
        /// </summary>
        public bool CanCancelCommands => true;

        /// <summary>
        /// Gets or sets the trace SQL flag
        /// </summary>
        public bool TraceSql { get; set; }

        /// <inheritdoc/>
        public IDbStatementFactory StatementFactory { get; }

        /// <summary>
        /// Static CTOR
        /// </summary>
        static SqliteProvider()
        {
            // We use reflection because we don't compile SQLite as a hard dependency (like other providers)
            if (!TryInitializeWithBatteries())
            {
                if (!TryInitializeDirectly())
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.METHOD_NOT_FOUND, "SetProvider"));
                }
            }
        }

        /// <summary>
        /// Try to initialize the sqlite provider directly using the well known package names for sqlite.
        /// </summary>
        /// <returns></returns>
        private static bool TryInitializeDirectly()
        {
            //This solution should be replaced in the future with a more elegant initializer system which is
            //capable of inspecting multiple providers and working for all .NET platforms.

            var rawtype = Type.GetType("SQLitePCL.raw, SQLitePCLRaw.core");

            if (null == rawtype)
            {
                return false;
            }

            var setprovidermethod = rawtype.GetMethod("SetProvider", BindingFlags.Static | BindingFlags.Public);

            if (null == setprovidermethod)
            {
                return false;
            }


            var sqlite3mcprovidertype = Type.GetType("SQLitePCL.SQLite3Provider_e_sqlite3mc,SQLitePCLRaw.provider.e_sqlite3mc", throwOnError: false);

            if (null != sqlite3mcprovidertype)
            {
                var provider = Activator.CreateInstance(sqlite3mcprovidertype);

                setprovidermethod.Invoke(null, new[] { provider });

                return true;
            }


            var cdeclprovidertype = Type.GetType("SQLitePCL.SQLite3Provider_dynamic_cdecl,SQLitePCLRaw.provider.dynamic_cdecl", throwOnError: false);

            if (null != cdeclprovidertype)
            {
                var setupmethod = cdeclprovidertype.GetMethod("Setup", BindingFlags.Public | BindingFlags.Static, Type.DefaultBinder, new[] { typeof(string), typeof(IGetFunctionPointer) }, null);

                var functionloader = new SqliteFunctionLoader();

                setupmethod.Invoke(null, new object[] { "e_sqlite3mc", (IGetFunctionPointer)functionloader });

                var provider = Activator.CreateInstance(cdeclprovidertype);

                setprovidermethod.Invoke(null, new[] { provider });

                return true;
            }

            return false;
        }

        /// <summary>
        /// Try to initialize the sqlite provider using a batteries initializer.
        /// </summary>
        /// <returns>True if initialize suceeded, false otherwise.</returns>
        private static bool TryInitializeWithBatteries()
        {
            var providerBatteries = Type.GetType("SQLitePCL.Batteries, SQLitePCLRaw.batteries_v2") ?? Type.GetType("SQLitePCL.Batteries, SQLitePCLRaw.batteries");
            if (providerBatteries == null)
            {
                return false;
                //throw new InvalidOperationException(String.Format(ErrorMessages.TYPE_NOT_FOUND, "SQLitePCL.Batteries"));
            }

            var sqliteInitializeMethod = providerBatteries?.GetRuntimeMethod("Init", Type.EmptyTypes);
            if (sqliteInitializeMethod == null)
            {
                return false;
                //throw new InvalidOperationException(String.Format(ErrorMessages.METHOD_NOT_FOUND, "SetProvider"));
            }

            sqliteInitializeMethod.Invoke(null, new object[0]);

            return true;
        }

        /// <summary>
        /// New sqlite provider
        /// </summary>
        public SqliteProvider()
        {
            this.StatementFactory = new SqliteStatementFactory(this);

        }

        /// <summary>
        /// Get the name of the provider
        /// </summary>
        public string Invariant => InvariantName;

        /// <summary>
        /// Gets ht emonitor probe
        /// </summary>
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
        /// Fire progress changed event
        /// </summary>
        protected void FireProgressChanged(ProgressChangedEventArgs e)
        {
            this.ProgressChanged?.Invoke(this, e);
        }

        /// <summary>
        /// Set the connection string password from a private key if it is set
        /// </summary>
        internal static ConnectionString CorrectConnectionString(ConnectionString connectionString)
        {
            var retVal = connectionString.Clone();

            var dataSource = retVal.GetComponent("Data Source");
            if (!String.IsNullOrEmpty(dataSource)
               && !dataSource.StartsWith("|DataDirectory|")
               && !Path.IsPathRooted(dataSource))
            {
                dataSource = $" |DataDirectory|\\{connectionString.GetComponent("Data Source")}";
            }
            dataSource = dataSource.Replace("|DataDirectory|", AppDomain.CurrentDomain.GetData("DataDirectory").ToString()).Replace("\\", Path.DirectorySeparatorChar.ToString());
            retVal.SetComponent("Data Source", dataSource);

            return retVal;
        }


        /// <summary>
        /// Create a command from the specified contxt with sql statement
        /// </summary>
        public IDbCommand CreateCommand(DataContext context, SqlStatement stmt)
        {
            var c = stmt.Prepare();
            if(this.TraceSql) {
                this.m_tracer.TraceVerbose(stmt.ToLiteral());
            }
            return CreateCommandInternal(context, CommandType.Text, c.Sql, c.Arguments.ToArray());
        }

        /// <summary>
        /// Create command from string and params
        /// </summary>
        public IDbCommand CreateCommand(DataContext context, string sql, params object[] parms)
        {
            return CreateCommandInternal(context, CommandType.Text, sql, parms);
        }

        /// <summary>
        /// Create stored procedure command
        /// </summary>
        public IDbCommand CreateStoredProcedureCommand(DataContext context, string spName, params object[] parms)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Create command internally
        /// </summary>
        private IDbCommand CreateCommandInternal(DataContext context, CommandType type, string sql, params object[] parms)
        {
            var cmd = context.Connection.CreateCommand();
            cmd.CommandType = type;

            var pno = 0;
            sql = m_parmRegex
               .Replace(sql, o => $"@parm{pno++} ")
               .Replace(" ILIKE ", " LIKE ");
            sql = m_uuidRegex
                .Replace(sql, o => $"x'{BitConverter.ToString(Guid.Parse(o.Groups[1].Value.Substring(1, 36)).ToByteArray()).Replace("-", "")}'");
            sql = m_dateRegex.Replace(sql, o => $"unixepoch({o.Groups[1].Value})");
            sql = m_limitOffsetRegex.Replace(sql, o =>
            {
                if (String.IsNullOrEmpty(o.Groups[2].Value)) // No limit
                {
                    return $" LIMIT {Int32.MaxValue} {o.Groups[1].Value}";
                }
                else
                {
                    return $" {o.Groups[2].Value} {o.Groups[1].Value}"; // swap
                }
            });
            cmd.CommandText = sql;
            cmd.Transaction = context.Transaction;

           
            pno = 0;
            foreach (var itm in parms)
            {
                var parm = cmd.CreateParameter();
                var value = itm;

                // Parameter type
                parm.DbType = MapParameterType(value?.GetType());
                parm.ParameterName = $"@parm{pno++}";
                if (value is DateTime && itm != null)
                {
                    parm.Value = ConvertValue(itm, typeof(long));
                }
                else if (value is DateTimeOffset && itm != null)
                {
                    parm.Value = ConvertValue(itm, typeof(long));
                }
                else if ((value is Guid || value is Guid?) && itm != null)
                {
                    parm.Value = ((Guid)itm).ToByteArray();
                }

                // Set value
                if (itm == null)
                {
                    parm.Value = DBNull.Value;
                }
                else if (itm.GetType().IsEnum)
                {
                    parm.Value = (int)itm;
                }
                else if (parm.Value == null)
                {
                    parm.Value = itm;
                }

                parm.Direction = ParameterDirection.Input;

              

                cmd.Parameters.Add(parm);
            }

            return cmd;
        }


        /// <summary>
        /// Static type map cache. Contains the same types that were checked with a per-command type check. Enum types will also be cached in this dictionary.
        /// </summary>
        private static Dictionary<Type, DbType> s_DbTypeMap = new Dictionary<Type, DbType>()
        {
            {typeof(string), DbType.String },
            {typeof(DateTime), DbType.Int32 },
            {typeof(DateTimeOffset), DbType.Int32 },
            {typeof(int), DbType.Int32 },
            {typeof(long), DbType.Int64 },
            {typeof(bool), DbType.Boolean },
            {typeof(byte[]), DbType.Binary },
            {typeof(float), DbType.Double },
            {typeof(double), DbType.Double },
            {typeof(decimal), DbType.Decimal },
            {typeof(Guid), DbType.Binary },
            {typeof(DBNull), DbType.Object },
            {typeof(Nullable<DateTime>), DbType.Int32},
            {typeof(Nullable<DateTimeOffset>), DbType.Int32 },
            {typeof(Nullable<int>), DbType.Int32},
            {typeof(Nullable<long>), DbType.Int64 },
            {typeof(Nullable<bool>), DbType.Boolean },
            {typeof(Nullable<float>), DbType.Double },
            {typeof(Nullable<double>), DbType.Double },
            {typeof(Nullable<decimal>), DbType.Decimal },
            {typeof(Nullable<Guid>), DbType.Binary },
        };



        /// <summary>
        /// Map a parameter type from the provided type
        /// </summary>
        public DbType MapParameterType(Type type)
        {
            if (null == type)
            {
                return DbType.Object;
            }
            else if (s_DbTypeMap.TryGetValue(type, out var dbtype))
            {
                return dbtype;
            }
            else if (type.StripNullable().IsEnum)
            {
                s_DbTypeMap.Add(type, DbType.Int32);
                return DbType.Int32;
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(type), "Can't map parameter type");
            }

            //if (type == null)
            //{
            //    return DbType.Object;
            //}
            //else if (type.StripNullable() == typeof(string))
            //{
            //    return DbType.String;
            //}
            //else if (type.StripNullable() == typeof(DateTime))
            //{
            //    return DbType.Int32;
            //}
            //else if (type.StripNullable() == typeof(DateTimeOffset))
            //{
            //    return DbType.Int32;
            //}
            //else if (type.StripNullable() == typeof(int))
            //{
            //    return DbType.Int32;
            //}
            //else if (type.StripNullable() == typeof(long))
            //{
            //    return DbType.Int64;
            //}
            //else if (type.StripNullable() == typeof(bool))
            //{
            //    return DbType.Boolean;
            //}
            //else if (type.StripNullable() == typeof(byte[]))
            //{
            //    return DbType.Binary;
            //}
            //else if (type.StripNullable() == typeof(float) || type.StripNullable() == typeof(double))
            //{
            //    return DbType.Double;
            //}
            //else if (type.StripNullable() == typeof(decimal))
            //{
            //    return DbType.Decimal;
            //}
            //else if (type.StripNullable() == typeof(Guid))
            //{
            //    return DbType.Binary;
            //}
            //else if (type == typeof(DBNull))
            //{
            //    return DbType.Object;
            //}
            //else if (type.StripNullable().IsEnum)
            //{
            //    return DbType.Int32;
            //}
            //else
            //{
            //    throw new ArgumentOutOfRangeException(nameof(type), "Can't map parameter type");
            //}
        }

        /// <summary>
        /// Get provider factory
        /// </summary>
        /// <returns>The SQLite provider</returns>
        protected DbProviderFactory GetProviderFactory()
        {

            if (this.m_provider == null) // HACK for Mono - WHY IS THIS A HACK?
            {
                var provType = ApplicationServiceContext.Current?.GetService<IConfigurationManager>().GetSection<OrmConfigurationSection>().AdoProvider.Find(o => o.Invariant.Equals(this.Invariant, StringComparison.OrdinalIgnoreCase))?.Type
                    ?? Type.GetType(ProviderFactoryType);
                if (provType == null)
                {
                    throw new InvalidOperationException("Cannot find SQLite provider");
                }

                this.m_provider = provType.GetField("Instance").GetValue(null) as DbProviderFactory;
            }

            if (this.m_provider == null)
            {
                throw new InvalidOperationException("Missing SQLite provider");
            }

            return this.m_provider;
        }

        /// <summary>
        /// Gets read only connection
        /// </summary>
        public virtual DataContext GetReadonlyConnection()
        {
            // If a backup is in progress do not call
            this.m_lockoutEvent.Wait();
            var conn = this.GetProviderFactory().CreateConnection();
            var connectionString = CorrectConnectionString(new ConnectionString(InvariantName, this.ReadonlyConnectionString ?? this.ConnectionString));
            connectionString.SetComponent("Mode", "ReadOnly");
            connectionString.SetComponent("Cache", "Private");
            connectionString.SetComponent("Pooling", "True");
            conn.ConnectionString = connectionString.ToString();
            return new ReaderWriterLockingDataContext(this, conn, true);
        }

        /// <summary>
        /// Get a write connection
        /// </summary>
        /// <returns></returns>
        public virtual DataContext GetWriteConnection()
        {
            return this.GetWriteConnectionInternal();
        }

        /// <summary>
        /// Get write connection internal overide the foreign keys
        /// </summary>
        protected DataContext GetWriteConnectionInternal(bool? foreignKeys = null)
        {
            this.m_lockoutEvent.Wait();
            var conn = this.GetProviderFactory().CreateConnection();
            var connectionString = CorrectConnectionString(new ConnectionString(InvariantName, this.ReadonlyConnectionString ?? this.ConnectionString));
            connectionString.SetComponent("Mode", "ReadWriteCreate");
            connectionString.SetComponent("Cache", "Private");
            connectionString.SetComponent("Pooling", "False");
            if (foreignKeys.HasValue)
            {
                connectionString.SetComponent("Foreign Keys", foreignKeys.ToString());
            }

            conn.ConnectionString = connectionString.ToString();
            return new ReaderWriterLockingDataContext(this, conn, false);
        }

        ///// <summary>
        ///// Lock the specified connection
        ///// </summary>
        //public object Lock(IDbConnection connection)
        //{
        //    object _lock = null;
        //    if (!m_locks.TryGetValue(connection.ConnectionString, out _lock))
        //    {
        //        _lock = new object();
        //        lock (m_locks)
        //        {
        //            if (!m_locks.ContainsKey(connection.ConnectionString))
        //            {
        //                m_locks.Add(connection.ConnectionString, _lock);
        //            }
        //            else
        //            {
        //                return m_locks[connection.ConnectionString];
        //            }
        //        }
        //    }
        //    return _lock;
        //}

        /// <inheritdoc/>
        public T ConvertValue<T>(object value) => (T)this.ConvertValue(value, typeof(T));

        /// <summary>
        /// Convert object
        /// </summary>
        public object ConvertValue(object value, Type toType)
        {
            object retVal = null;
            if (value == null)
            {
                return null;
            }
            else if (typeof(DateTime) == toType.StripNullable() &&
                (value is int || value is long))
            {
                retVal = SanteDBConvert.ParseDateTime((long)value);
            }
            else if (typeof(DateTimeOffset) == toType.StripNullable() &&
                (value is int || value is long))
            {
                retVal = SanteDBConvert.ParseDateTimeOffset((long)value);
            }
            else if (typeof(long) == toType.StripNullable() &&
                value is DateTime dt)
            {
                retVal = SanteDBConvert.ToDateTime(dt);
            }
            else if (typeof(long) == toType.StripNullable() &&
                value is DateTimeOffset dto)
            {
                retVal = SanteDBConvert.ToDateTimeOffset(dto);
            }
            else if (typeof(Guid) == toType.StripNullable() &&
                value is byte[] bValue)
            {
                retVal = new Guid(bValue);
            }
            else if (typeof(byte[]) == toType.StripNullable() &&
                value is byte[] bValue2 &&
                bValue2.Length == 16) // this is a UUID - SQLite reads (esp from the data layer) are just BLOBs
            {
                retVal = new Guid(bValue2);
            }

            else
            {
                MapUtil.TryConvert(value, toType, out retVal);
            }

            return retVal;
        }

        /// <summary>
        /// Clone this connection
        /// </summary>
        public DataContext CloneConnection(DataContext source)
        {
            if (m_provider == null)
            {
                m_provider = Activator.CreateInstance(Type.GetType("System.Data.SQLite.SQLiteProviderFactory, System.Data.SQLite, Culture=nuetral")) as DbProviderFactory;
            }

            var retVal = this.GetReadonlyConnection();
            retVal.ContextId = source.ContextId;
            return retVal;

            //var conn = m_provider.CreateConnection();
            //conn.ConnectionString = source.Connection.ConnectionString;
            //return new DataContext(this, conn) { ContextId = source.ContextId };
        }



        /// <summary>
        /// Map datatype
        /// </summary>
        public string MapSchemaDataType(Type type)
        {
            type = type.StripNullable();
            if (type == typeof(byte[]))
            {
                return "blob";
            }
            else
            {
                switch (type.Name)
                {
                    case nameof(Boolean):
                        return "boolean";
                    case nameof(DateTime):
                    case nameof(DateTimeOffset):
                    case nameof(Int64):
                        return "BIGINT";
                    case nameof(Decimal):
                        return "DECIMAL";
                    case nameof(Single):
                    case nameof(Double):
                        return "REAL";
                    case nameof(Int32):
                        return "INTEGER";
                    case nameof(String):
                        return "VARCHAR(256)";
                    case nameof(Guid):
                        return "blob(16)";
                    default:
                        throw new NotSupportedException($"Schema type {type} not supported by SQLite provider");
                }
            }
        }


        /// <summary>
        /// Get the database name
        /// </summary>
        /// <returns></returns>
        public virtual string GetDatabaseName()
        {
            var filePath = CorrectConnectionString(new ConnectionString(this.Invariant, this.ConnectionString)).GetComponent("Data Source");
            if (Path.IsPathRooted(filePath))
            {
                return Path.GetFileName(filePath);
            }
            else
            {
                return filePath;
            }
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

                    connection.ConnectionString = CorrectConnectionString(new ConnectionString(InvariantName, this.ConnectionString)).ToString();
                    connection.Open();
                    byte[] aleSmk = null;
                    // Attempt to connect to the PostgreSQL encryption provider to get the secret
                    using (var cmd = connection.CreateCommand())
                    {

                        // Does the ALE systbl exist?
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE tbl_name = 'ALE_SYSTBL')";
                        if (!this.ConvertValue<bool>(cmd.ExecuteScalar())) // not initalized
                        {
                            return;
                        }

                        cmd.CommandText = "SELECT ale_smk FROM ale_systbl";
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
                            this.MigrateEncryption(null);
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
                    connection.ConnectionString = CorrectConnectionString(new ConnectionString(InvariantName, this.ConnectionString)).ToString();
                    connection.Open();

                    using (var cmd = connection.CreateCommand())
                    {

                        if (this.m_encryptionProvider != null) // no current encryption provider so just initialize
                        {
                            this.m_tracer.TraceInfo("Decrypting with old key...");
                            aleConfiguration.DisableAll();
                            this.m_encryptionSettings.AleRecrypt(this);
                            this.m_encryptionSettings = null;
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = "DELETE FROM ale_systbl;";
                            cmd.ExecuteNonQuery();
                        }

                        this.m_tracer.TraceInfo("Encrypting with new key...");
                        if (newOrmEncryptionSettings?.AleEnabled == true)
                        {
                            this.m_tracer.TraceWarning("GENERATING AN APPLICATION LEVEL ENCRYPTION CERTIFICATE -> IT IS RECOMMENDED YOU USE TDE RATHER THAN ALE ON SANTEDB PRODUCTION INSTANCES");
                            var aleSmk = DefaultAesDataEncryptor.GenerateMasterKey(newOrmEncryptionSettings);
                            cmd.CommandText = "INSERT INTO ale_systbl VALUES (@key)";
                            var parm = cmd.CreateParameter();
                            parm.ParameterName = "@key";
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

        /// <inheritdoc/>
        public bool BackupToStream(Stream backupStream)
        {
            try
            {
                // Grab a lock and prevent everyone from interacting with this database
                this.ClearAllPools();
                this.WalCheckpointInvoke();

                using (var rw = new ReaderWriterLockingDataContext(this, null))
                {
                    this.m_tracer.TraceInfo("Backing up {0} to provided stream", this.GetDatabaseName());
                    rw.Lock(); // Ensure that there are no active connections
                    this.m_lockoutEvent.Wait(); // Ensure that no other maintenance threads are doing any work
                    this.m_lockoutEvent.Reset();

                    var cstr = CorrectConnectionString(new ConnectionString(this.Invariant, this.ReadonlyConnectionString));
                    // First bytes for the password or NIL for no password
                    backupStream.WritePascalString(cstr.GetComponent("password"));

                    var rootFileName = cstr.GetComponent("Data Source");
                    var filesToBackup = Directory.GetFiles(Path.GetDirectoryName(rootFileName), Path.GetFileName(rootFileName) + "*");

                    // Emit the backup stuff
                    filesToBackup.ForEach(fileName =>
                    {
                        try
                        {
                            var assetName = Path.GetFileName(fileName);
                            this.m_tracer.TraceInfo("Writing {0}...", fileName);
                            using (var fs = File.OpenRead(fileName))
                            {
                                backupStream.WritePascalString(assetName);
                                backupStream.Write(BitConverter.GetBytes(fs.Length), 0, 8);
                                fs.CopyTo(backupStream);
                            }
                        }
                        catch (Exception e)
                        {
                            this.m_tracer.TraceWarning("Could not backup {0} - {1}", fileName, e.ToHumanReadableString());
                        }
                    });
                    return true;
                } // lock is released
            }
            finally
            {
                this.m_lockoutEvent.Set();
            }
        }

        /// <inheritdoc/>
        public bool RestoreFromStream(Stream restoreStream)
        {
            try
            {

                // Clear all pools
                this.ClearAllPools();
                this.WalCheckpointInvoke();

                // Grab a lock and prevent everyone from interacting with this database
                this.m_lockoutEvent.Wait();
                this.m_lockoutEvent.Reset();

                using (var rw = new ReaderWriterLockingDataContext(this, null))
                {
                    rw.Lock();

                    var tempFile = Path.GetTempFileName();
                    try
                    {
                        var cstr = CorrectConnectionString(new ConnectionString(this.Invariant, this.ReadonlyConnectionString));
                        var productionFile = cstr.GetComponent("Data Source");
                        var sourcePassword = restoreStream.ReadPascalString();
                        // First bytes for the password or NIL for no password

                        while (true)
                        {
                            var assetName = restoreStream.ReadPascalString();
                            if (String.IsNullOrEmpty(assetName)) break;
                            var assetBuffer = new byte[8];
                            restoreStream.Read(assetBuffer, 0, 8);
                            var assetSize = BitConverter.ToInt64(assetBuffer, 0);
                            var bytesRead = 0;
                            using (var fs = File.OpenWrite(tempFile))
                            {
                                while (bytesRead < assetSize)
                                {
                                    assetBuffer = new byte[assetSize - bytesRead > 16_384 ? 16_384 : assetSize - bytesRead];
                                    bytesRead += restoreStream.Read(assetBuffer, 0, assetBuffer.Length);
                                    fs.Write(assetBuffer, 0, assetBuffer.Length);
                                }
                            }

                            try
                            {
                                var destinationFile = Path.Combine(Path.GetDirectoryName(productionFile), Path.GetFileName(assetName));
                                File.Copy(tempFile, destinationFile, true); // Move the file to replace the original
                            }
                            catch (Exception e)
                            {
                                this.m_tracer.TraceWarning("Cannot restore database file {0}", assetName);
                            }
                            finally
                            {
                                File.Delete(tempFile);
                            }
                        }


                        // Does the database need to be re-keyed?
                        var myPassword = cstr.GetComponent("password");
                        if (!String.IsNullOrEmpty(sourcePassword) && !sourcePassword.Equals(myPassword))
                        {
                            using (var conn = this.GetProviderFactory().CreateConnection())
                            {
                                conn.ConnectionString = $"Data Source={tempFile}; Password={sourcePassword}";
                                conn.Open();
                                using (var c = conn.CreateCommand())
                                {
                                    c.CommandText = "SELECT quote($password)";
                                    var passwordParm = c.CreateParameter();
                                    passwordParm.ParameterName = "$password";
                                    passwordParm.Value = myPassword;
                                    c.Parameters.Add(passwordParm);
                                    c.CommandText = $"PRAGMA rekey = {c.ExecuteScalar()}";
                                    c.Parameters.Clear();
                                    c.ExecuteNonQuery();
                                }
                            }
                        }

                        return true;
                    }
                    finally
                    {
                        File.Delete(tempFile);
                    }
                }
            }
            finally
            {
                this.m_lockoutEvent.Set();
            }
        }

        /// <summary>
        /// Dispose all waiting handles
        /// </summary>
        public virtual void Dispose()
        {
            this.WalCheckpointInvoke();
        }

        /// <inheritdoc>
        public virtual void Optimize()
        {
            this.ClearAllPools();
            using (var writer = this.GetWriteConnection())
            {
                try
                {
                    this.m_tracer.TraceInfo("Optimizing {0}...", this.GetDatabaseName());

                    this.m_lockoutEvent.Wait();
                    this.m_lockoutEvent.Reset();
                    writer.Open(initializeExtensions: false);
                    writer.ExecuteNonQuery(this.StatementFactory.CreateSqlKeyword(SqlKeyword.Vacuum));
                    writer.ExecuteNonQuery(this.StatementFactory.CreateSqlKeyword(SqlKeyword.Reindex));
                    writer.ExecuteNonQuery(this.StatementFactory.CreateSqlKeyword(SqlKeyword.Analyze));
                    writer.ExecuteNonQuery("PRAGMA wal_checkpoint(truncate)");
                }
                finally
                {
                    this.m_lockoutEvent.Set();
                }
            }
        }

        /// <summary>
        /// Invoke a WAL checkpoint
        /// </summary>
        private void WalCheckpointInvoke()
        {
            this.m_tracer.TraceInfo("Performing WAL Checkpoint on {0}", this.GetDatabaseName());
            using (var writer = this.GetWriteConnection())
            {
                writer.Open(initializeExtensions: false);
                writer.ExecuteNonQuery("PRAGMA wal_checkpoint(truncate)");
            }
            this.ClearAllPools();
        }

        /// <summary>
        /// Clear all pools
        /// </summary>
        protected void ClearAllPools()
        {
            this.GetProviderFactory().CreateConnection().GetType().GetMethod("ClearAllPools").Invoke(null, new object[0]);
        }

        /// <summary>
        /// Implement this later
        /// </summary>
        public virtual IEnumerable<DbStatementReport> StatActivity()
        {
            yield break;
        }

        /// <inheritdoc/>
        public virtual void InitializeConnection(IDbConnection conn)
        {

            if (ApplicationServiceContext.Current.HostType == SanteDBHostType.Client)
            {
                conn.Execute("PRAGMA journal_mode=MEMORY");
                conn.Execute("PRAGMA synchronous=OFF");
                conn.Execute("PRAGMA temp_store=MEMORY");
                conn.Execute("PRAGMA ignore_check_constraints=ON");
                conn.Execute("PRAGMA recursive_triggers=OFF");
                conn.Execute("PRAGMA foreign_keys=FALSE");
            }
            else
            {
                conn.Execute("PRAGMA journal_mode=WAL");
                conn.ExecuteScalar<Object>("PRAGMA synchronous=normal");
            }

            conn.ExecuteScalar<Object>("PRAGMA locking_mode=normal");
            conn.ExecuteScalar<object>("PRAGMA pragma_automatic_index=true");

        }
    }
}