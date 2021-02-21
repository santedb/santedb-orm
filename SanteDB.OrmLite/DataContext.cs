/*
 * Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
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
 * Date: 2021-2-9
 */
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Map;
using SanteDB.Core.Model.Warehouse;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Represents a data context
    /// </summary>
    public partial class DataContext : IDisposable
    {
       
        // the connection
        private IDbConnection m_connection;

        // the transaction
        private IDbTransaction m_transaction;

        // The provider
        private IDbProvider m_provider;

        // Data dictionary
        private ConcurrentDictionary<String, Object> m_dataDictionary = new ConcurrentDictionary<string, object>();

        // Items to be added to cache after an action
        private ConcurrentDictionary<Guid, IdentifiedData> m_cacheCommit = new ConcurrentDictionary<Guid, IdentifiedData>();

        // Trace source
        private Tracer m_tracer = new Tracer(Constants.TracerName);

        // Commands prepared on this connection
        private ConcurrentDictionary<String, IDbCommand> m_preparedCommands = new ConcurrentDictionary<string, IDbCommand>();

        /// <summary>
        /// Connection
        /// </summary>
        public IDbConnection Connection { get { return this.m_connection; } }

        /// <summary>
        /// Load state
        /// </summary>
        public LoadState LoadState { get; set; }

        /// <summary>
        /// Data dictionary
        /// </summary>
        public IDictionary<String, Object> Data { get { return this.m_dataDictionary; } }

        /// <summary>
        /// Internal utility method to get provider
        /// </summary>
        internal IDbProvider Provider => this.m_provider;

        /// <summary>
        /// Cache on commit
        /// </summary>
        public IEnumerable<IdentifiedData> CacheOnCommit
        {
            get
            {
                return this.m_cacheCommit.Values;
            }
        }

        /// <summary>
        /// True if the context should prepare statements
        /// </summary>
        public bool PrepareStatements { get; set; }

        /// <summary>
        /// Current Transaction
        /// </summary>
        public IDbTransaction Transaction { get { return this.m_transaction; } }

        /// <summary>
        /// Query builder
        /// </summary>
        public QueryBuilder GetQueryBuilder(ModelMapper map)
        {
            return new QueryBuilder(map, this.m_provider);
        }


        /// <summary>
        /// Creates a new data context
        /// </summary>
        public DataContext(IDbProvider provider, IDbConnection connection)
        {
            this.ContextId = Guid.NewGuid();
            this.m_provider = provider;
            this.m_connection = connection;
        }

        /// <summary>
        /// Creates a new data context
        /// </summary>
        public DataContext(IDbProvider provider, IDbConnection connection, bool isReadonly)
        {
            this.ContextId = Guid.NewGuid();
            this.m_provider = provider;
            this.m_connection = connection;
            this.IsReadonly = isReadonly;
        }

        /// <summary>
        /// Creates a new data context
        /// </summary>
        public DataContext(IDbProvider provider, IDbConnection connection, IDbTransaction tx) : this(provider, connection)
        {
            this.ContextId = Guid.NewGuid();
            this.m_transaction = tx;
        }

        /// <summary>
        /// Begin a transaction
        /// </summary>
        public IDbTransaction BeginTransaction()
        {
            if (this.m_transaction == null)
                this.m_transaction = this.m_connection.BeginTransaction();
            return this.m_transaction;
        }


        /// <summary>
        /// Get the datatype
        /// </summary>
        public String GetDataType(SchemaPropertyType type)
        {
            return this.m_provider.MapDatatype(type);
        }

        /// <summary>
        /// Open the connection to the database
        /// </summary>
        public void Open()
        {
            this.m_tracer.TraceEvent(EventLevel.Verbose, "Connecting to {0}...", this.m_connection.ConnectionString);
            if (this.m_connection.State == ConnectionState.Closed)
                this.m_connection.Open();
            else if (this.m_connection.State == ConnectionState.Broken)
            {
                this.m_connection.Close();
                this.m_connection.Open();
            }
            else if (this.m_connection.State != ConnectionState.Open)
                this.m_connection.Open();

            // Can set timeouts
            if (this.m_provider.Features.HasFlag(SqlEngineFeatures.SetTimeout))
                try
                {
                    using (var cmd = this.m_connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = "SET statement_timeout to 60000";
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceWarning("Error setting timeout: {0}", e);
                }
                finally // Sometimes the psql server will cack the connection 
                {
                    if (this.m_connection.State != ConnectionState.Open)
                        this.m_connection.Open();
                }

        }

        /// <summary>
        /// Add a prepared command
        /// </summary>
        internal void AddPreparedCommand(IDbCommand cmd)
        {
            this.m_preparedCommands.TryAdd(cmd.CommandText, cmd);
        }

        /// <summary>
        /// Opens a cloned context
        /// </summary>
        public DataContext OpenClonedContext()
        {
            if (this.Transaction != null)
                throw new InvalidOperationException("Cannot clone connection in transaction");
            var retVal = this.m_provider.CloneConnection(this);
            retVal.Open();
            retVal.m_dataDictionary = this.m_dataDictionary; // share data
            retVal.LoadState = this.LoadState;
            //retVal.PrepareStatements = this.PrepareStatements;
            return retVal;
        }

        /// <summary>
        /// Get prepared command
        /// </summary>
        internal IDbCommand GetPreparedCommand(string sql)
        {
            IDbCommand retVal = null;
            this.m_preparedCommands.TryGetValue(sql, out retVal);
            return retVal;
        }

        /// <summary>
        /// True if the command is prepared
        /// </summary>
        internal bool IsPreparedCommand(IDbCommand cmd)
        {
            IDbCommand retVal = null;
            return this.m_preparedCommands.TryGetValue(cmd.CommandText, out retVal) && retVal == cmd;
        }

        /// <summary>
        /// Dispose this object
        /// </summary>
        public void Dispose()
        {
            if (this.m_preparedCommands != null)
                foreach (var itm in this.m_preparedCommands.Values)
                {

                    try
                    {
                        itm.Cancel();
                    }
                    catch { }

                    itm?.Dispose();
                }
            if (this.m_lastCommand != null)
            {
                try { if (this.m_provider.CanCancelCommands) this.m_lastCommand?.Cancel(); }
                catch { }
                finally { this.m_lastCommand?.Dispose(); this.m_lastCommand = null;  }
            }

            this.m_cacheCommit?.Clear();
            this.m_cacheCommit = null;
            this.m_transaction?.Dispose();
            this.m_transaction = null;
            this.m_connection?.Dispose();
            this.m_connection = null;
            this.m_dataDictionary?.Clear();
            this.m_dataDictionary = null;
        }

        /// <summary>
        /// Add cache commit
        /// </summary>
        public void AddCacheCommit(IdentifiedData data)
        {
            try
            {
                IdentifiedData existing = null;
                if (data.Key.HasValue && !this.m_cacheCommit.TryGetValue(data.Key.Value, out existing))
                {
                    this.m_cacheCommit.TryAdd(data.Key.Value, data);
                }
                else if (data.Key.HasValue && data.LoadState > (existing?.LoadState ?? 0))
                    this.m_cacheCommit[data.Key.Value] = data;
            }
            catch (Exception e)
            {
                this.m_tracer.TraceEvent(EventLevel.Warning, "Object {0} won't be added to cache: {1}", data, e);
            }
        }


        /// <summary>
        /// Add cache commit
        /// </summary>
        public IdentifiedData GetCacheCommit(Guid key)
        {
            IdentifiedData retVal = null;
            this.m_cacheCommit.TryGetValue(key, out retVal);
            return retVal;
        }

        /// <summary>
        /// Create sql statement
        /// </summary>
        public SqlStatement CreateSqlStatement()
        {
            return new SqlStatement(this.m_provider);
        }

        /// <summary>
        /// Create sql statement
        /// </summary>
        public SqlStatement CreateSqlStatement(String sql, params object[] args)
        {
            return new SqlStatement(this.m_provider, sql, args);
        }

        /// <summary>
        /// Create SQL statement
        /// </summary>
        public SqlStatement<T> CreateSqlStatement<T>()
        {
            return new SqlStatement<T>(this.m_provider);
        }

        /// <summary>
        /// Query
        /// </summary>
        public String GetQueryLiteral(SqlStatement query)
        {
            query = query.Build();
            StringBuilder retVal = new StringBuilder(query.SQL);
            String sql = retVal.ToString();
            var qList = query.Arguments?.ToArray() ?? new object[0];
            int parmId = 0;
            int lastIndex = 0;
            while (sql.IndexOf("?", lastIndex) > -1)
            {
                var pIndex = sql.IndexOf("?", lastIndex);
                retVal.Remove(pIndex, 1);
                var obj = qList[parmId++];
                if (obj is String || obj is Guid || obj is Guid? || obj is DateTime || obj is DateTimeOffset)
                    obj = $"'{obj}'";
                else if (obj == null)
                    obj = "null";
                retVal.Insert(pIndex, obj);
                sql = retVal.ToString();
                lastIndex = pIndex + obj.ToString().Length;
            }
            return retVal.ToString();
        }

    }
}
