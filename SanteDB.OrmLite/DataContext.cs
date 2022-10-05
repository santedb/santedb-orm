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
 * Date: 2022-5-30
 */
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Model.Map;
using SanteDB.OrmLite.Diagnostics;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
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
        private bool m_opened;

        // Trace source
        private Tracer m_tracer = new Tracer(Constants.TracerName);


        /// <summary>
        /// Increment the value
        /// </summary>
        private void IncrementProbe(OrmPerformanceMetric metric)
        {
            if (this.m_provider is IDbMonitorProvider idmp && idmp.MonitorProbe is OrmClientProbe ocp)
            {
                ocp.Increment(metric);
            }
        }

        /// <summary>
        /// Decrement the value
        /// </summary>
        private void DecrementProbe(OrmPerformanceMetric metric)
        {
            if (this.m_provider is IDbMonitorProvider idmp && idmp.MonitorProbe is OrmClientProbe ocp)
            {
                ocp.Decrement(metric);
            }
        }

        /// <summary>
        /// Average the time
        /// </summary>
        private void AddProbeResponseTime(long value)
        {
            if (this.m_provider is IDbMonitorProvider idmp && idmp.MonitorProbe is OrmClientProbe ocp)
            {
                ocp.AverageWith(OrmPerformanceMetric.AverageTime, value);
            }
        }

        /// <summary>
        /// Connection
        /// </summary>
        public IDbConnection Connection
        { get { return this.m_connection; } }

        /// <summary>
        /// Temporary lookup values used during this context's use
        /// </summary>
        public IDictionary<String, Object> Data
        { get { return this.m_dataDictionary; } }

        /// <summary>
        /// Overrides the command timeout for any command executed on this data context
        /// </summary>
        public int? CommandTimeout { get; set; }

        /// <summary>
        /// Internal utility method to get provider
        /// </summary>
        internal IDbProvider Provider => this.m_provider;

        /// <summary>
        /// Current Transaction
        /// </summary>
        public IDbTransaction Transaction
        { get { return this.m_transaction; } }

        /// <summary>
        /// Query builder
        /// </summary>
        public QueryBuilder GetQueryBuilder(ModelMapper map)
        {
            return new QueryBuilder(map, this.m_provider.StatementFactory);
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
            {
                this.m_transaction = this.m_connection.BeginTransaction();
            }

            return this.m_transaction;
        }

        /// <summary>
        /// Close the connection however don't dispose
        /// </summary>
        public void Close()
        {
            this.ThrowIfDisposed();
            if (this.m_transaction != null)
            {
                this.m_transaction.Rollback();
                this.m_transaction.Dispose();
            }
            if (this.m_opened)
            {
                this.DecrementProbe(this.IsReadonly ? OrmPerformanceMetric.ReadonlyConnections : OrmPerformanceMetric.ReadWriteConnections);
                this.m_opened = false;
            }
            this.m_connection.Close();
        }

        /// <summary>
        /// Open the connection to the database
        /// </summary>
        public void Open()
        {
            this.ThrowIfDisposed();

            if (this.m_opened)
            {
                this.DecrementProbe(this.IsReadonly ? OrmPerformanceMetric.ReadonlyConnections : OrmPerformanceMetric.ReadWriteConnections);
            }

            if (this.m_connection.State == ConnectionState.Closed)
            {
                if (this.m_opened)
                {
                    this.DecrementProbe(this.IsReadonly ? OrmPerformanceMetric.ReadonlyConnections : OrmPerformanceMetric.ReadWriteConnections);
                }

                this.m_connection.Open();
                this.IncrementProbe(this.IsReadonly ? OrmPerformanceMetric.ReadonlyConnections : OrmPerformanceMetric.ReadWriteConnections);
            }
            else if (this.m_connection.State == ConnectionState.Broken)
            {
                this.m_connection.Close();
                this.m_connection.Open();
            }
            else if (this.m_connection.State != ConnectionState.Open)
            {
                if (this.m_opened)
                {
                    this.DecrementProbe(this.IsReadonly ? OrmPerformanceMetric.ReadonlyConnections : OrmPerformanceMetric.ReadWriteConnections);
                }

                this.m_connection.Open();
                this.IncrementProbe(this.IsReadonly ? OrmPerformanceMetric.ReadonlyConnections : OrmPerformanceMetric.ReadWriteConnections);

            }
            this.m_opened = true;
        }

        /// <summary>
        /// Opens a cloned context
        /// </summary>
        public DataContext OpenClonedContext()
        {

            var retVal = this.m_provider.CloneConnection(this);
            retVal.Open();
            retVal.m_dataDictionary = this.m_dataDictionary; // share data
            //retVal.PrepareStatements = this.PrepareStatements;
            return retVal;
        }

        /// <summary>
        /// Dispose this object
        /// </summary>
        public void Dispose()
        {

            if (this.m_lastCommand != null)
            {
                try
                {
                    if (this.m_provider.CanCancelCommands)
                    {
                        this.m_lastCommand?.Cancel();
                    }
                }
                catch { }
                finally { this.m_lastCommand?.Dispose(); this.m_lastCommand = null; }
            }

            if (this.m_connection != null && this.m_opened)
            {
                this.DecrementProbe(this.IsReadonly ? OrmPerformanceMetric.ReadonlyConnections : OrmPerformanceMetric.ReadWriteConnections);
            }

            this.m_transaction?.Dispose();
            this.m_transaction = null;
            this.m_connection?.Dispose();
            this.m_connection = null;
            this.m_dataDictionary?.Clear();
            this.m_dataDictionary = null;

        }

        /// <summary>
        /// Throw an exception if this context is disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (this.m_connection == null)
            {
                throw new ObjectDisposedException(nameof(DataContext));
            }
        }
        /// <summary>
        /// Create sql statement
        /// </summary>
        public SqlStatement CreateSqlStatement()
        {
            return new SqlStatement(this.m_provider.StatementFactory);
        }

        /// <summary>
        /// Create sql statement
        /// </summary>
        public SqlStatement CreateSqlStatement(String sql, params object[] args)
        {
            return new SqlStatement(this.m_provider.StatementFactory, sql, args);
        }

        /// <summary>
        /// Create SQL statement
        /// </summary>
        public SqlStatement<T> CreateSqlStatement<T>()
        {
            return new SqlStatement<T>(this.m_provider.StatementFactory);
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
                {
                    obj = $"'{obj}'";
                }
                else if (obj == null)
                {
                    obj = "null";
                }

                retVal.Insert(pIndex, obj);
                sql = retVal.ToString();
                lastIndex = pIndex + obj.ToString().Length;
            }
            return retVal.ToString();
        }
    }
}