/*
 * Copyright (C) 2021 - 2022, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-8-27
 */
using SanteDB.Core;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Interfaces;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Map;
using SanteDB.Core.Model.Warehouse;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// Represents a IDbProvider for PostgreSQL
    /// </summary>
    public class PostgreSQLProvider : IDbMonitorProvider
    {
        // Last rr host used
        private int m_lastRrHost = 0;

        // Readonly IP Addresses
        private IPAddress[] m_readonlyIpAddresses;

        // Trace source
        private Tracer m_tracer = new Tracer(Constants.TracerName + ".PostgreSQL");

        // DB provider factory
        private DbProviderFactory m_provider = null;

        // Filter functions
        private static Dictionary<String, IDbFilterFunction> s_filterFunctions = null;

        // Index functions
        private static Dictionary<String, IDbIndexFunction> s_indexFunctions = null;

        /// <summary>
        /// Create new provider
        /// </summary>
        public PostgreSQLProvider()
        {
            this.MonitorProbe = Diagnostics.OrmClientProbe.CreateProbe(this);

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
        /// SQL Engine features
        /// </summary>
        public SqlEngineFeatures Features
        {
            get
            {
                return SqlEngineFeatures.AutoGenerateGuids |
                    SqlEngineFeatures.AutoGenerateTimestamps |
                    SqlEngineFeatures.ReturnedInsertsAsReader |
                    SqlEngineFeatures.StrictSubQueryColumnNames |
                    SqlEngineFeatures.LimitOffset |
                    SqlEngineFeatures.FetchOffset |
                    SqlEngineFeatures.MustNameSubQuery |
                    SqlEngineFeatures.SetTimeout |
                    SqlEngineFeatures.MaterializedViews;
            }
        }

        /// <summary>
        /// Get name of provider
        /// </summary>
        public string Invariant => "npgsql";

        /// <summary>
        /// Get the monitor probe
        /// </summary>
        public IDiagnosticsProbe MonitorProbe { get; }

        /// <summary>
        /// Get provider factory
        /// </summary>
        /// <returns></returns>
        private DbProviderFactory GetProviderFactory()
        {
            if (this.m_provider == null) // HACK for Mono
            {
                var provType = ApplicationServiceContext.Current?.GetService<IConfigurationManager>().GetSection<OrmConfigurationSection>().AdoProvider.Find(o => o.Invariant.Equals(this.Invariant, StringComparison.OrdinalIgnoreCase))?.Type
                    ?? Type.GetType("Npgsql.NpgsqlFactory, Npgsql");
                if (provType == null)
                    throw new InvalidOperationException("Cannot find NPGSQL provider");
                this.m_provider = provType.GetField("Instance").GetValue(null) as DbProviderFactory;
            }
            if (this.m_provider == null)
                throw new InvalidOperationException("Missing Npgsql provider");
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
            var finStmt = stmt.Build();

#if DB_DEBUG
            if(System.Diagnostics.Debugger.IsAttached)
                this.Explain(context, CommandType.Text, finStmt.SQL, finStmt.Arguments.ToArray());
#endif

            return this.CreateCommandInternal(context, CommandType.Text, finStmt.SQL, finStmt.Arguments.ToArray());
        }

        /// <summary>
        /// Perform an explain query
        /// </summary>
        private void Explain(DataContext context, CommandType text, string sQL, object[] v)
        {
            using (var cmd = this.CreateCommandInternal(context, CommandType.Text, "EXPLAIN " + sQL, v))
            using (var plan = cmd.ExecuteReader())
                while (plan.Read())
                {
                    if (plan.GetValue(0).ToString().Contains("Seq"))
                        System.Diagnostics.Debugger.Break();
                }
        }

        // Parameter regex
        private readonly Regex m_parmRegex = new Regex(@"\?");

        /// <summary>
        /// Create command internally
        /// </summary>
        private IDbCommand CreateCommandInternal(DataContext context, CommandType type, String sql, params object[] parms)
        {
            var pno = 0;

            sql = this.m_parmRegex.Replace(sql, o => $"@parm{pno++}");

            if (pno != parms.Length && type == CommandType.Text)
                throw new ArgumentOutOfRangeException(nameof(sql), $"Parameter mismatch query expected {pno} but {parms.Length} supplied");

            IDbCommand cmd = context.Connection.CreateCommand();
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
                    parm.Value = DBNull.Value;
                else if (value?.GetType().IsEnum == true)
                    parm.Value = (int)value;
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
                    parm.Value = itm;

                if (type == CommandType.Text)
                    parm.ParameterName = $"parm{pno++}";
                parm.Direction = ParameterDirection.Input;

                if (this.TraceSql)
                    this.m_tracer.TraceEvent(EventLevel.Verbose, "\t [{0}] {1} ({2})", cmd.Parameters.Count, parm.Value, parm.DbType);

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
            if (type == null) return DbType.Object;

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

                default:
                    if (type.StripNullable() == typeof(byte[])) return System.Data.DbType.Binary;
                    else if (type.StripNullable().IsEnum) return DbType.Int32;
                    else throw new ArgumentOutOfRangeException(nameof(type), $"Can't map parameter type {type.Name}");
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

        /// <summary>
        /// Return exists
        /// </summary>
        public SqlStatement Count(SqlStatement sqlStatement)
        {
            return new SqlStatement(this, "SELECT COUNT(*) FROM (").Append(sqlStatement.Build()).Append(") Q0");
        }

        /// <summary>
        /// Return exists
        /// </summary>
        public SqlStatement Exists(SqlStatement sqlStatement)
        {
            return new SqlStatement(this, "SELECT CASE WHEN EXISTS (").Append(sqlStatement.Build()).Append(") THEN true ELSE false END");
        }

        /// <summary>
        /// Append a returning statement
        /// </summary>
        public SqlStatement Returning(SqlStatement sqlStatement, params ColumnMapping[] returnColumns)
        {
            if (returnColumns.Length == 0)
                return sqlStatement;
            return sqlStatement.Append($" RETURNING {String.Join(",", returnColumns.Select(o => o.Name))}");
        }

        /// <summary>
        /// Gets a lock
        /// </summary>
        public object Lock(IDbConnection conn)
        {
            return new object();
        }

        /// <summary>
        /// Convert value just uses the mapper if needed
        /// </summary>
        public object ConvertValue(object value, Type toType)
        {
            object retVal = null;
            if (value != DBNull.Value)
            {
                if (toType.IsAssignableFrom(value.GetType()))
                    return value;
                else
                    MapUtil.TryConvert(value, toType, out retVal);
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
        /// Create SQL keyword
        /// </summary>
        public string CreateSqlKeyword(SqlKeyword keywordType)
        {
            switch (keywordType)
            {
                case SqlKeyword.ILike:
                    return " ILIKE ";

                case SqlKeyword.Like:
                    return " LIKE ";

                case SqlKeyword.Lower:
                    return " LOWER ";

                case SqlKeyword.Upper:
                    return " UPPER ";

                case SqlKeyword.False:
                    return " FALSE ";

                case SqlKeyword.True:
                    return " TRUE ";

                case SqlKeyword.CreateOrAlter:
                    return "CREATE OR REPLACE ";

                case SqlKeyword.RefreshMaterializedView:
                    return "REFRESH MATERIALIZED VIEW ";

                default:
                    throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Map datatype
        /// </summary>
        public string MapSchemaDataType(Type type)
        {
            type = type.StripNullable();
            if (type == typeof(byte[]))
                return "BYTEA";
            else switch (type.Name)
                {
                    case nameof(Boolean):
                        return "BOOLEAN";
                    case nameof(DateTime):
                        return "DATE";
                    case nameof(DateTimeOffset):
                        return "TIMESTAMPTZ";
                    case nameof(Decimal):
                        return "DECIMAL";
                    case nameof(Double):
                        return "FLOAT";
                    case nameof(Int32):
                        return "INTEGER";
                    case nameof(Int64):
                        return "BIGINT";
                    case nameof(String):
                        return "VARCHAR(256)";
                    case nameof(Guid):
                        return "UUID";
                    default:
                        throw new NotSupportedException($"Schema type {type} not supported by PostgreSQL provider");
                }

        }

        /// <summary>
        /// Gets the filter function
        /// </summary>
        public IDbFilterFunction GetFilterFunction(string name)
        {
            if (s_filterFunctions == null)
            {
                s_filterFunctions = ApplicationServiceContext.Current.GetService<IServiceManager>()
                        .CreateInjectedOfAll<IDbFilterFunction>()
                        .Where(o => o.Provider == "pgsql")
                        .ToDictionary(o => o.Name, o => o);
            }
            IDbFilterFunction retVal = null;
            s_filterFunctions.TryGetValue(name, out retVal);
            return retVal;
        }

        /// <summary>
        /// Gets the index function
        /// </summary>
        public IDbIndexFunction GetIndexFunction(string name)
        {
            if (s_indexFunctions == null)
            {
                s_indexFunctions = ApplicationServiceContext.Current.GetService<IServiceManager>()
                        .CreateInjectedOfAll<IDbIndexFunction>()
                        .Where(o => o.Provider == this.Invariant)
                        .ToDictionary(o => o.Name, o => o);
            }

            s_indexFunctions.TryGetValue(name, out var retVal);
            return retVal;
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
                        while (rdr.Read())
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

        /// <summary>
        /// Get reset sequence command
        /// </summary>
        public SqlStatement GetResetSequence(string sequenceName, object sequenceValue)
        {
            return new SqlStatement(this, $"SELECT setval('{sequenceName}', {sequenceValue})");
        }

        /// <inheritdoc/>
        public SqlStatement CreateIndex(string indexName, string tableName, string column, bool isUnique)
        {
            return new SqlStatement(this, $"CREATE {(isUnique ? "UNIQUE" : "")} INDEX {indexName} ON {tableName} USING BTREE ({column})");
        }

        /// <inheritdoc/>
        public SqlStatement DropIndex(string indexName)
        {
            return new SqlStatement(this, $"DROP INDEX {indexName};");
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
    }
}