/*
 * Copyright 2015-2018 Mohawk College of Applied Arts and Technology
 *
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
 * User: justin
 * Date: 2018-2-9
 */

/*
 * This product includes software developed by Borland Software Corp.
 */
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SanteDB.Core.Model.Map;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.IO;
using SanteDB.Core.Model;
using System.Diagnostics;
using SanteDB.Core.Model.Warehouse;

namespace SanteDB.OrmLite.Providers.Firebird
{
    /// <summary>
    /// Represents a FirebirdSQL provider
    /// </summary>
    public class FirebirdSQLProvider : IDbProvider
    {

        // Trace source
        private TraceSource m_tracer = new TraceSource(Constants.TracerName + ".FirebirdSQL");

        // DB provider factory
        private DbProviderFactory m_provider = null;

        // Parameter regex
        private readonly Regex m_parmRegex = new Regex(@"\?");

        // Filter functions
        private static Dictionary<String, IDbFilterFunction> s_filterFunctions = new Dictionary<string, IDbFilterFunction>();

        /// <summary>
        /// Gets or sets the connection string for the provider
        /// </summary>
        public string ConnectionString { get; set; }

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
        public string Name
        {
            get
            {
                return "fbsql";
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
        /// Clone a connection
        /// </summary>
        /// <param name="source">The connection context to clone</param>
        /// <returns>The cloned connection</returns>
        public DataContext CloneConnection(DataContext source)
        {
            return source.IsReadonly ? this.GetReadonlyConnection() : this.GetWriteConnection();
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

            if (pno != parms.Length && type == CommandType.Text)
                throw new ArgumentOutOfRangeException(nameof(sql), $"Parameter mismatch query expected {pno} but {parms.Length} supplied");


            IDbCommand cmd = context.GetPreparedCommand(sql);
            if (cmd == null)
            {
                cmd = context.Connection.CreateCommand();
                cmd.Transaction = context.Transaction;
                cmd.CommandType = type;

                if (this.TraceSql)
                    this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "[{0}] {1}", type, sql);

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
                        this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "\t [{0}] {1} ({2})", cmd.Parameters.Count, parm.Value, parm.DbType);


                    cmd.Parameters.Add(parm);
                }

                cmd.CommandText = sql;

                // Prepare command
                if (context.PrepareStatements && !cmd.CommandText.StartsWith("EXPLAIN"))
                {
                    if (!cmd.Parameters.OfType<IDataParameter>().Any(o => o.DbType == DbType.Object) &&
                        context.Transaction == null)
                        cmd.Prepare();

                    context.AddPreparedCommand(cmd);
                }
            }
            else
            {
                if (cmd.Parameters.Count != parms.Length)
                    throw new ArgumentOutOfRangeException(nameof(parms), "Argument count mis-match");

                for (int i = 0; i < parms.Length; i++)
                    (cmd.Parameters[i] as IDataParameter).Value = parms[i] ?? DBNull.Value;
            }

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
            else if (type.StripNullable() == typeof(Boolean)) return System.Data.DbType.Boolean;
            else if (type.StripNullable() == typeof(byte[]))
                return System.Data.DbType.Binary;
            else if (type.StripNullable() == typeof(float) || type.StripNullable() == typeof(double)) return System.Data.DbType.Double;
            else if (type.StripNullable() == typeof(Decimal)) return System.Data.DbType.Decimal;
            else if (type.StripNullable() == typeof(Guid)) return DbType.String;
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
                    return "LIKE";
                case SqlKeyword.Lower:
                    return "LOWER";
                case SqlKeyword.Upper:
                    return "UPPER";
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
                this.m_provider = DbProviderFactories.GetFactory("Fbsql");

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
            // HACK: FBSQL doesn't understand || parameters
            cstring.ConnectionString = this.ConnectionString; //.Replace("|DataDirectory|", AppDomain.CurrentDomain.GetData("DataDirectory").ToString());
            if (!cstring.ContainsKey("ClientLibrary"))
                cstring.Add("ClientLibrary", Path.Combine(Path.GetDirectoryName(typeof(FirebirdSQLProvider).Assembly.Location), "fbclient.dll"));
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
        public string MapDatatype(SchemaPropertyType type)
        {
            switch (type)
            {
                case SchemaPropertyType.Binary:
                    return "BLOB";
                case SchemaPropertyType.Boolean:
                    return "BOOLEAN";
                case SchemaPropertyType.Date:
                    return "DATE";
                case SchemaPropertyType.TimeStamp:
                case SchemaPropertyType.DateTime:
                    return "TIMESTAMP";
                case SchemaPropertyType.Decimal:
                    return "DECIMAL";
                case SchemaPropertyType.Float:
                    return "FLOAT";
                case SchemaPropertyType.Integer:
                    return "BIGINT";
                case SchemaPropertyType.String:
                    return "VARCHAR(256)";
                case SchemaPropertyType.Uuid:
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
                s_filterFunctions = AppDomain.CurrentDomain.GetAssemblies().SelectMany(a => a.ExportedTypes)
                        .Where(t => typeof(IDbFilterFunction).IsAssignableFrom(t) && !t.IsAbstract)
                        .Select(t => Activator.CreateInstance(t) as IDbFilterFunction)
                        .Where(o => o.Provider == "pgsql")
                        .ToDictionary(o => o.Name, o => o);
            }
            IDbFilterFunction retVal = null;
            s_filterFunctions.TryGetValue(name, out retVal);
            return retVal;
        }
    }
}
