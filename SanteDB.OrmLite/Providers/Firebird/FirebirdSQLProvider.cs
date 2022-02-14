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

/*
 * This product includes software developed by Borland Software Corp.
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
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite.Providers.Firebird
{
    /// <summary>
    /// Represents a FirebirdSQL provider
    /// </summary>
    public class FirebirdSQLProvider : IDbMonitorProvider
    {

        // Trace source
        private Tracer m_tracer = new Tracer(Constants.TracerName + ".FirebirdSQL");

        // DB provider factory
        private DbProviderFactory m_provider = null;


        // Parameter regex
        private readonly Regex m_parmRegex = new Regex(@"\?");

        // UUID regex
        private readonly Regex m_uuidRegex = new Regex(@"(\'[A-Za-z0-9]{8}\-[A-Za-z0-9]{4}\-[A-Za-z0-9]{4}\-[A-Za-z0-9]{4}\-[A-Za-z0-9]{12}\')");

        // Filter functions
        private static Dictionary<String, IDbFilterFunction> s_filterFunctions = null;

        // Filter functions
        private static Dictionary<String, IDbIndexFunction> s_indexFunctions = null;

        private IDiagnosticsProbe m_monitor;

        /// <summary>
        /// Create a new firebird provider
        /// </summary>
        public FirebirdSQLProvider()
        {
        }

        /// <summary>
        /// Gets or sets the connection string for the provider
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// True if commands can be cancel
        /// </summary>
        public bool CanCancelCommands => false;

        /// <summary>
        /// Gets the features that this provider 
        /// </summary>
        public SqlEngineFeatures Features
        {
            get
            {
                return SqlEngineFeatures.AutoGenerateTimestamps |
                    SqlEngineFeatures.FetchOffset |
                    SqlEngineFeatures.ReturnedInsertsAsParms |
                    SqlEngineFeatures.StrictSubQueryColumnNames;
            }
        }

        /// <summary>
        /// Gets the name of the provider
        /// </summary>
        public string Invariant
        {
            get
            {
                return "FirebirdSQL";
            }
        }

        /// <summary>
        /// Gets or sets the readonly connection string
        /// </summary>
        public string ReadonlyConnectionString { get; set; }

        /// <summary>
        /// Gets or sets whether SQL tracing is supported
        /// </summary>
        public bool TraceSql { get; set; }

        /// <summary>
        /// Get hte monitoring probe
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
        /// Clone a connection
        /// </summary>
        /// <param name="source">The connection context to clone</param>
        /// <returns>The cloned connection</returns>
        public DataContext CloneConnection(DataContext source)
        {
            var retVal = source.IsReadonly ? this.GetReadonlyConnection() : this.GetWriteConnection();
            retVal.ContextId = source.ContextId;
            return retVal;
        }

        /// <summary>
        /// Convert a value to the specified type
        /// </summary>
        /// <param name="toType">The type to convert to</param>
        /// <param name="value">The value to be converted</param>
        public object ConvertValue(object value, Type toType)
        {
            object retVal = null;
            if (value != DBNull.Value)
            {
                // Hack: Firebird handles UUIDs as a char array of 16 rather than a byte array
                if (toType.StripNullable() == typeof(Guid))
                    retVal = Guid.Parse(String.Join("", Encoding.Default.GetBytes(value.ToString()).Select(o => (o).ToString("x2")).ToArray()));
                else if (toType.IsAssignableFrom(value.GetType()))
                    return value;
                else if (value is DateTime dt && toType.StripNullable().Equals(typeof(DateTimeOffset)))
                    return (DateTimeOffset)dt;
                else if (!MapUtil.TryConvert(value, toType, out retVal))
                    throw new ArgumentOutOfRangeException(nameof(value), $"Cannot convert {value?.GetType().Name} to {toType.Name}");
            }
            return retVal;
        }

        /// <summary>
        /// Turn the specified SQL statement into a count statement
        /// </summary>
        /// <param name="sqlStatement">The SQL statement to be counted</param>
        /// <returns>The count statement</returns>
        public SqlStatement Count(SqlStatement sqlStatement)
        {
            return new SqlStatement(this, "SELECT COUNT(*) FROM (").Append(sqlStatement.Build()).Append(") Q0");
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
        /// Create command internally
        /// </summary>
        private IDbCommand CreateCommandInternal(DataContext context, CommandType type, String sql, params object[] parms)
        {

            var pno = 0;

            sql = this.m_parmRegex
                .Replace(sql, o => $"@parm{pno++} ")
                .Replace(" ILIKE ", " LIKE ");
            sql = this.m_uuidRegex
                .Replace(sql, o => $"char_to_uuid({o.Groups[1].Value})")
                .Replace("char_to_uuid(char_to_uuid(", "(char_to_uuid("); //HACK:

            if (pno != parms.Length && type == CommandType.Text)
                throw new ArgumentOutOfRangeException(nameof(sql), $"Parameter mismatch query expected {pno} but {parms.Length} supplied");


            var cmd = context.Connection.CreateCommand();
            cmd.Transaction = context.Transaction;
            cmd.CommandType = type;

            if (this.TraceSql)
                this.m_tracer.TraceEvent(EventLevel.Verbose, "[{0}] {1}", type, sql);

            pno = 0;
            foreach (var itm in parms)
            {
                var parm = cmd.CreateParameter();
                var value = itm;

                // Parameter type
                parm.DbType = this.MapParameterType(value?.GetType());

                // Set value
                if (itm == null)
                    parm.Value = DBNull.Value;
                else if (value?.GetType().IsEnum == true)
                    parm.Value = (int)value;
                else if (parm.DbType == DbType.DateTime && value is DateTimeOffset dto)
                    parm.Value = dto.DateTime;
                else
                    parm.Value = itm;

                if (type == CommandType.Text)
                    parm.ParameterName = $"parm{pno++}";

                // Compensate UUID
                if (value is Guid || value is Guid?)
                {
                    sql = sql.Replace($"@{parm.ParameterName} ", $"char_to_uuid(@{parm.ParameterName}) ");
                    parm.DbType = System.Data.DbType.String;
                }

                parm.Direction = ParameterDirection.Input;

                if (this.TraceSql)
                    this.m_tracer.TraceEvent(EventLevel.Verbose, "\t [{0}] {1} ({2})", cmd.Parameters.Count, parm.Value, parm.DbType);


                cmd.Parameters.Add(parm);
            }

            cmd.CommandText = sql;



            return cmd;
        }

        /// <summary>
        /// Map a parameter type from the provided type
        /// </summary>
        public DbType MapParameterType(Type type)
        {
            if (type == null) return DbType.Object;
            else if (type.StripNullable() == typeof(String)) return System.Data.DbType.String;
            else if (type.StripNullable() == typeof(DateTime)) return System.Data.DbType.DateTime;
            else if (type.StripNullable() == typeof(DateTimeOffset)) return DbType.DateTime;
            else if (type.StripNullable() == typeof(Int32)) return System.Data.DbType.Int32;
            else if (type.StripNullable() == typeof(Int64)) return System.Data.DbType.Int64;
            else if (type.StripNullable() == typeof(Boolean)) return System.Data.DbType.Boolean;
            else if (type.StripNullable() == typeof(byte[]))
                return System.Data.DbType.Binary;
            else if (type.StripNullable() == typeof(float) || type.StripNullable() == typeof(double)) return System.Data.DbType.Double;
            else if (type.StripNullable() == typeof(Decimal)) return System.Data.DbType.Decimal;
            else if (type.StripNullable() == typeof(TimeSpan)) return System.Data.DbType.Time;
            else if (type.StripNullable() == typeof(Guid)) return DbType.String;
            else if (type.IsEnum) return DbType.Int32;
            else if (type == typeof(DBNull))
                return DbType.Object;
            else
                throw new ArgumentOutOfRangeException(nameof(type), "Can't map parameter type");
        }

        /// <summary>
        /// Create a command
        /// </summary>
        /// <param name="context">The data context to create the command on</param>
        /// <param name="sql">The SQL contents</param>
        /// <param name="parms">The parameter values</param>
        /// <returns>The constructed command</returns>
        public IDbCommand CreateCommand(DataContext context, string sql, params object[] parms)
        {
            return this.CreateCommandInternal(context, CommandType.Text, sql, parms);

        }

        /// <summary>
        /// Create SQL keyword
        /// </summary>
        /// <param name="keywordType">The type of keyword</param>
        /// <returns>The SQL equivalent</returns>
        public string CreateSqlKeyword(SqlKeyword keywordType)
        {
            switch (keywordType)
            {
                case SqlKeyword.ILike:
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
                    return "CREATE OR ALTER ";
                default:
                    throw new ArgumentOutOfRangeException(nameof(keywordType));
            }
        }

        /// <summary>
        /// Create a stored procedure execution 
        /// </summary>
        /// <param name="context">The context of the command</param>
        /// <param name="spName">The stored procedure name</param>
        /// <param name="parms">The parameters to be created</param>
        /// <returns>The constructed command object</returns>
        public IDbCommand CreateStoredProcedureCommand(DataContext context, string spName, params object[] parms)
        {
            return this.CreateCommandInternal(context, CommandType.StoredProcedure, spName, parms);
        }

        /// <summary>
        /// Create an EXISTS statement
        /// </summary>
        /// <param name="sqlStatement">The statement to determine EXISTS on</param>
        /// <returns>The constructed statement</returns>
        public SqlStatement Exists(SqlStatement sqlStatement)
        {
            return new SqlStatement(this, "SELECT CASE WHEN EXISTS (").Append(sqlStatement.Build()).Append(") THEN true ELSE false END FROM RDB$DATABASE");
        }

        /// <summary>
        /// Get provider factory
        /// </summary>
        /// <returns>The FirebirdSQL provider </returns>
        private DbProviderFactory GetProviderFactory()
        {
            if (this.m_provider == null) // HACK for Mono
            {
                var provType = ApplicationServiceContext.Current?.GetService<IConfigurationManager>().GetSection<OrmConfigurationSection>().AdoProvider.Find(o => o.Invariant.Equals(this.Invariant, StringComparison.OrdinalIgnoreCase))?.Type
                    ?? Type.GetType("FirebirdSql.Data.FirebirdClient.FirebirdClientFactory, FirebirdSql.Data.FirebirdClient");
                if (provType == null)
                    throw new InvalidOperationException("Cannot find FirebirdSQL provider");
                this.m_provider = provType.GetField("Instance").GetValue(null) as DbProviderFactory;
            }


            if (this.m_provider == null)
                throw new InvalidOperationException("Missing FirebirdSQL provider");
            return this.m_provider;
        }

        /// <summary>
        /// Correc connection string client library
        /// </summary>
        private String CorrectConnectionStringLib()
        {
            var cstring = new DbConnectionStringBuilder();
            // HACK: FirebirdSQL doesn't understand || parameters
            cstring.ConnectionString = this.ConnectionString.Replace("|DataDirectory|", AppDomain.CurrentDomain.GetData("DataDirectory").ToString());
            if (!cstring.ContainsKey("ClientLibrary"))
                cstring.Add("ClientLibrary", Path.Combine(Path.GetDirectoryName(typeof(FirebirdSQLProvider).Assembly.Location), "fbclient.dll"));

            if (!cstring.ContainsKey("Charset"))
                cstring.Add("Charset", "NONE");
            return cstring.ConnectionString;
        }

        /// <summary>
        /// Get a readonly connection
        /// </summary>
        public DataContext GetReadonlyConnection()
        {
            var conn = this.GetProviderFactory().CreateConnection();
            conn.ConnectionString = this.CorrectConnectionStringLib();
            return new DataContext(this, conn, true);
        }

        /// <summary>
        /// Get a write connection
        /// </summary>
        /// <returns></returns>
        public DataContext GetWriteConnection()
        {
            var conn = this.GetProviderFactory().CreateConnection();
            conn.ConnectionString = this.CorrectConnectionStringLib();
            return new DataContext(this, conn, false);
        }

        /// <summary>
        /// Get a lock object for the specified database connection
        /// </summary>
        /// <param name="connection">The connection to lock</param>
        /// <returns>The lock object for the connection</returns>
        public object Lock(IDbConnection connection)
        {
            return new object();
        }

        /// <summary>
        /// Maps the specified data type
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public string MapSchemaDataType(Type type)
        {
            type = type.StripNullable();
            if (type == typeof(byte[]))
                return "BLOB";
            else switch (type.Name)
                {
                    case nameof(Boolean):
                        return "BOOLEAN";
                    case nameof(DateTime):
                        return "DATE";
                    case nameof(DateTimeOffset):
                        return "TIMESTAMP";
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
                        throw new NotSupportedException($"Schema type {type} not supported by FirebirdSQL provider");
                }
        }

        /// <summary>
        /// Perform a returning command
        /// </summary>
        /// <param name="sqlStatement">The SQL statement to "return"</param>
        /// <param name="returnColumns">The columns to return</param>
        /// <returns>The returned colums</returns>
        public SqlStatement Returning(SqlStatement sqlStatement, params ColumnMapping[] returnColumns)
        {
            if (returnColumns.Length == 0)
                return sqlStatement;
            return sqlStatement.Append($" RETURNING {String.Join(",", returnColumns.Select(o => o.Name))}");

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
                        .Where(o => o.Provider == this.Invariant)
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
        /// Stat the activity
        /// </summary>
        /// <returns></returns>
        public IEnumerable<DbStatementReport> StatActivity()
        {
            using (var conn = this.GetReadonlyConnection())
            {
                conn.Open();
                using (var cmd = conn.Connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "select mon$statement_id as pid, mon$state as state, mon$timestamp as query_start, cast(left(mon$sql_text, 128) as varchar(128)) as query from mon$statements;";
                    using (var rdr = cmd.ExecuteReader())
                        while (rdr.Read())
                            yield return new DbStatementReport()
                            {
                                StatementId = rdr["pid"].ToString(),
                                Start = DateTime.Parse(rdr["query_start"].ToString()),
                                Status = rdr["state"].ToString() == "1" ? DbStatementStatus.Active : DbStatementStatus.Idle,
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
            return new SqlStatement(this, $"ALTER SEQUENCE {sequenceName} RESTART WITH {(int)sequenceValue}");
        }

        /// <inheritdoc/>
        public SqlStatement CreateIndex(string indexName, string tableName, string column, bool isUnique)
        {
            return new SqlStatement(this, $"CREATE {(isUnique ? "UNIQUE" : "")} INDEX {indexName} ON {tableName} ({column})");
        }

        /// <inheritdoc/>
        public SqlStatement DropIndex(string indexName)
        {
            return new SqlStatement(this, $"DROP INDEX {indexName}");
        }


        /// <summary>
        /// Get the name of the database
        /// </summary>
        public string GetDatabaseName()
        {
            var fact = this.GetProviderFactory().CreateConnectionStringBuilder();
            fact.ConnectionString = this.ConnectionString;
            fact.TryGetValue("initial catalog", out var value);
            return value?.ToString();
        }

    }
}
