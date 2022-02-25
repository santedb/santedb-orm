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

using System.Reflection;
using SanteDB.Core.i18n;
using SanteDB.Core.Model;
using SanteDB.OrmLite.Attributes;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;

namespace SanteDB.OrmLite
{

    /// <summary>
    /// Data context functions for the execution of query data
    /// </summary>
    public partial class DataContext
    {
        // Lock
        private object m_lockObject = new object();

        // Base types
        private static readonly HashSet<Type> BaseTypes = new HashSet<Type>()
        {
            typeof(bool),
            typeof(bool?),
            typeof(int),
            typeof(int?),
            typeof(float),
            typeof(float?),
            typeof(double),
            typeof(double?),
            typeof(decimal),
            typeof(decimal?),
            typeof(String),
            typeof(Guid),
            typeof(Guid?),
            typeof(Type),
            typeof(DateTime),
            typeof(DateTime?),
            typeof(DateTimeOffset),
            typeof(DateTimeOffset?),
            typeof(UInt32),
            typeof(UInt32?),
            typeof(byte[])
        };

        /// <summary>
        /// True if the connection is readonly
        /// </summary>
        public bool IsReadonly { get; private set; }

        /// <summary>
        /// Gets or sets the context id
        /// </summary>
        public Guid ContextId { get; set; }

        // Last command
        private IDbCommand m_lastCommand = null;

        /// <summary>
        /// Execute a stored procedure transposing the result set back to <typeparamref name="TModel"/>
        /// </summary>
        public IEnumerable<TModel> Query<TModel>(String spName, params object[] arguments)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject) // Ensures only one query is being executed on this context at a time
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateStoredProcedureCommand(this, spName, arguments))
                    {
                        try
                        {
                            this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);
                            using (var rdr = dbc.ExecuteReader())
                            {
                                while (rdr.Read())
                                {
                                    yield return this.MapObject<TModel>(rdr);
                                }
                            }
                        }
                        finally
                        {
                            this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);
                        }
                    }
                }
#if DEBUG 
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(EventLevel.Verbose, "QUERY {0} executed in {1} ms", spName, sw.ElapsedMilliseconds);
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
            }
#endif
        }

#if DBPERF
        private static object s_lockObject = new object();
        /// <summary>
        /// Performance monitor
        /// </summary>
        private void PerformanceMonitor(SqlStatement stmt, Stopwatch sw)
        {
            sw.Stop();
            if(sw.ElapsedMilliseconds > 5)
            {
                lock(s_lockObject)
                {
                    using (var tw = File.AppendText("dbperf.xml"))
                    {
                        tw.WriteLine($"<sql><cmd>{this.GetQueryLiteral(stmt.Build())}</cmd><elapsed>{sw.ElapsedMilliseconds}</elapsed>");
                        tw.WriteLine($"<stack><[!CDATA[{new System.Diagnostics.StackTrace(true).ToString()}]]></stack><plan><![CDATA[");
                        stmt = this.CreateSqlStatement("EXPLAIN ").Append(stmt);
                        using (var dbc = this.m_provider.CreateCommand(this, stmt))
                            using (var rdr = dbc.ExecuteReader())
                                while (rdr.Read())
                                    tw.WriteLine(rdr[0].ToString());
                        tw.WriteLine("]]></plan></sql>");
                    }
                }
            }
            sw.Start();
        }
#endif

        /// <summary>
        /// Determines if <paramref name="obj"/> exists in the database
        /// </summary>
        public bool Exists<TModel>(TModel obj)
        {
            var sqlStatement = this.CreateSqlStatement();
            foreach (var cm in TableMapping.Get(typeof(TModel)).Columns.Where(o => o.IsPrimaryKey))
                sqlStatement.And($"{cm.Name} = ?", cm.SourceProperty.GetValue(obj));
            sqlStatement = this.CreateSqlStatement<TModel>().SelectFrom(ColumnMapping.One).Where(sqlStatement);
            return this.Any(sqlStatement);
        }

        /// <summary>
        /// Determines if <paramref name="modelKey"/> of type <paramref name="type"/> exists in the database
        /// </summary>
        public bool Exists(Type type, Guid modelKey)
        {
            var sqlStatement = this.CreateSqlStatement();
            foreach (var cm in TableMapping.Get(type).Columns.Where(o => o.IsPrimaryKey))
                sqlStatement.And($"{cm.Name} = ?", modelKey);
            sqlStatement = this.CreateSqlStatement().SelectFrom(type, ColumnMapping.One).Where(sqlStatement);
            return this.Any(sqlStatement);
        }

        /// <summary>
        /// Map an object
        /// </summary>
        private TModel MapObject<TModel>(IDataReader rdr)
        {
            if (typeof(CompositeResult).IsAssignableFrom(typeof(TModel)))
            {
                var retVal = Activator.CreateInstance(typeof(TModel));
                (retVal as CompositeResult).ParseValues(rdr, this.m_provider);
                return (TModel)retVal;
            }
            else if (BaseTypes.Contains(typeof(TModel)))
            {
                var obj = rdr[0];
                if (obj == DBNull.Value)
                    return default(TModel);
                else if (typeof(TModel).IsAssignableFrom(obj.GetType()))
                    return (TModel)obj;
                else
                    return (TModel)this.m_provider.ConvertValue(obj, typeof(TModel));
            }
            else if (typeof(Object) == typeof(TModel)) // Any old object
                return (TModel)this.m_provider.ConvertValue(rdr[0], typeof(TModel));
            else if (typeof(ExpandoObject).IsAssignableFrom(typeof(TModel)))
                return this.MapExpando<TModel>(rdr);
            else
                return (TModel)this.MapObject(typeof(TModel), rdr);
        }

        /// <summary>
        /// Map expando object
        /// </summary>
        private TModel MapExpando<TModel>(IDataReader rdr)
        {
            var retVal = new ExpandoObject() as IDictionary<String, Object>;
            for (int i = 0; i < rdr.FieldCount; i++)
            {
                var value = rdr[i];
                if (value == DBNull.Value)
                    value = null;
                var name = rdr.GetName(i).ToLowerInvariant();
                retVal.Add(name, value);
            }
            return (TModel)retVal;
        }

        /// <summary>
        /// Map an object
        /// </summary>
        private object MapObject(Type tModel, IDataReader rdr)
        {
            var tableMapping = TableMapping.Get(tModel);
            object result = Activator.CreateInstance(tModel);
            // Read each column and pull from reader
            foreach (var itm in tableMapping.Columns)
            {
                try
                {
                    object value = this.m_provider.ConvertValue(rdr[itm.Name], itm.SourceProperty.PropertyType);
                    if (!itm.IsSecret)
                        itm.SourceProperty.SetValue(result, value);
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceEvent(EventLevel.Error, "Error mapping: {0} : {1}", itm.Name, e.ToString());
                    throw new MissingFieldException(tableMapping.TableName, itm.Name);
                }
            }

            return result;
        }

        /// <summary>
        /// Resets the specified sequence according to the logic in the provider
        /// </summary>
        public void ResetSequence(string sequenceName, object sequenceValue)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, this.m_provider.GetResetSequence(sequenceName, sequenceValue)))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                dbc.ExecuteNonQuery();
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                        finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(EventLevel.Verbose, "RESET SEQUENCE{0} to {1}", sequenceName, sequenceValue);
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// First or default returns only the first object or null if not found
        /// </summary>
        public Object FirstOrDefault(Type returnType, SqlStatement stmt)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt.Limit(1)))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }
                            using (var rdr = dbc.ExecuteReader())
                                    return this.ReaderToResult(returnType, rdr);
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(EventLevel.Verbose, "QUERY {0} executed in {1} ms", this.GetQueryLiteral(stmt), sw.ElapsedMilliseconds);
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);


            }
#endif
        }

        /// <summary>
        /// First or default returns only the first object or null if not found
        /// </summary>
        public TModel ExecuteProcedure<TModel>(String spName, params object[] arguments)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateStoredProcedureCommand(this, spName, arguments))
                    {
                        try
                        {
                            if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }
                            using (var rdr = dbc.ExecuteReader())
                                    return this.ReaderToResult<TModel>(rdr);
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                    }
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "FIRST {0} executed in {1} ms", spName, sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// First or default returns only the first object or null if not found
        /// </summary>
        public TModel FirstOrDefault<TModel>(Expression<Func<TModel, bool>> querySpec)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var stmt = this.CreateSqlStatement<TModel>().SelectFrom().Where(querySpec).Limit(1);
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }
                            using (var rdr = dbc.ExecuteReader())
                                    return this.ReaderToResult<TModel>(rdr);
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "FIRST {0} executed in {1} ms", querySpec, sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// First or default returns only the first object or null if not found
        /// </summary>
        public TModel FirstOrDefault<TModel>(SqlStatement stmt)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt.Build().Limit(1)))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }

                            using (var rdr = dbc.ExecuteReader())
                                    return this.ReaderToResult<TModel>(rdr);
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "FIRST {0} executed in {1} ms", this.GetQueryLiteral(stmt), sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Returns only if only one result is available
        /// </summary>
        public TModel SingleOrDefault<TModel>(Expression<Func<TModel, bool>> querySpec)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var stmt = this.CreateSqlStatement<TModel>().SelectFrom().Where(querySpec).Limit(2);

                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }

                            using (var rdr = dbc.ExecuteReader())
                            {
                                var retVal = this.ReaderToResult<TModel>(rdr);
                                if (!rdr.Read()) return retVal;
                                else throw new InvalidOperationException("Sequence contains more than one element");
                            }
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "SINGLE {0} executed in {1} ms", querySpec, sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Returns only if only one result is available
        /// </summary>
        public TReturn ExecuteScalar<TReturn>(SqlStatement sqlStatement)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, sqlStatement))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }

                            return (TReturn)dbc.ExecuteScalar();
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "SCALAR {0} executed in {1} ms", sqlStatement, sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Returns only if only one result is available
        /// </summary>
        public bool Any<TModel>(Expression<Func<TModel, bool>> querySpec)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var stmt = this.m_provider.Exists(this.CreateSqlStatement<TModel>().SelectFrom(ColumnMapping.One).Where(querySpec));
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }
                            return (bool)dbc.ExecuteScalar();
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "ANY {0} executed in {1} ms", querySpec, sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Represents the count function
        /// </summary>
        public bool Any(SqlStatement querySpec)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var stmt = this.m_provider.Exists(querySpec);
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }

                            return (bool)dbc.ExecuteScalar();
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "ANY {0} executed in {1} ms", this.GetQueryLiteral(querySpec), sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Returns only if only one result is available
        /// </summary>
        public long Count<TModel>(Expression<Func<TModel, bool>> querySpec)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var stmt = this.m_provider.Count(this.CreateSqlStatement<TModel>().SelectFrom().Where(querySpec));
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }

                            return (long)dbc.ExecuteScalar();
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "COUNT {0} executed in {1} ms", querySpec, sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Represents the count function
        /// </summary>
        public int Count(SqlStatement querySpec)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var stmt = this.m_provider.Count(querySpec);
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt))
                    {
                        try
                        {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }

                            return Convert.ToInt32(dbc.ExecuteScalar());
                        }
                        catch (TimeoutException)
                        {
                            try { dbc.Cancel(); } catch { }
                            throw;
                        }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }

                        }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "COUNT {0} executed in {1} ms", this.GetQueryLiteral(querySpec), sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Parse to a single result
        /// </summary>
        private TModel ReaderToResult<TModel>(IDataReader rdr)
        {
            if (rdr.Read()) return this.MapObject<TModel>(rdr);
            else return default(TModel);
        }

        /// <summary>
        /// Parse to a single result
        /// </summary>
        private object ReaderToResult(Type returnType, IDataReader rdr)
        {
            if (rdr.Read()) return this.MapObject(returnType, rdr);
            else return null;
        }

        /// <summary>
        /// Execute the specified query
        /// </summary>
        public OrmResultSet<TModel> Query<TModel>(Expression<Func<TModel, bool>> querySpec)
        {
            return new OrmResultSet<TModel>(this, this.CreateSqlStatement<TModel>().SelectFrom().Where(querySpec));
        }

        /// <summary>
        /// Query using the specified statement
        /// </summary>
        public OrmResultSet<TModel> Query<TModel>(SqlStatement query)
        {
            return new OrmResultSet<TModel>(this, query);
        }

        /// <summary>
        /// Executes the query against the database
        /// </summary>
        public IEnumerable<TModel> ExecQuery<TModel>(SqlStatement query)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, query))
                    {
                            try
                            {
                                this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                if (this.CommandTimeout.HasValue)
                                {
                                    dbc.CommandTimeout = this.CommandTimeout.Value;
                                }
                                using (var rdr = dbc.ExecuteReader())
                                    while (rdr.Read())
                                        yield return this.MapObject<TModel>(rdr);
                            }
                            finally
                            {
                                this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            }
                        }
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "QUERY {0} executed in {1} ms", this.GetQueryLiteral(query), sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Bulk insert data
        /// </summary>
        public IEnumerable<TModel> InsertOrUpdateAll<TModel>(IEnumerable<TModel> source)
        {
            return source.Select(this.InsertOrUpdate).ToList();
        }

        /// <summary>
        /// Insert or update the specifed object
        /// </summary>
        public TModel InsertOrUpdate<TModel>(TModel source)
        {
            return this.Exists(source) ? this.Update(source) : this.Insert(source);
        }

        /// <summary>
        /// Bulk insert data
        /// </summary>
        public IEnumerable<TModel> InsertAll<TModel>(IEnumerable<TModel> source)
        {
            return source.Select(o => this.Insert(o)).ToList();
        }

        /// <summary>
        /// Bulk update data
        /// </summary>
        public IEnumerable<TModel> UpdateAll<TModel>(IEnumerable<TModel> source)
        {
            return source.Select(o => this.Update(o)).ToList();
        }

        /// <summary>
        /// Bulk update data
        /// </summary>
        public IEnumerable<TModel> UpdateAll<TModel>(IEnumerable<TModel> source, Func<TModel, TModel> changor)
        {
            return source.Select(o => this.Update(changor(o))).ToList();
        }

        /// <summary>
        /// Insert the specified object
        /// </summary>
        public TModel Insert<TModel>(TModel value)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                // First we want to map object to columns
                var tableMap = TableMapping.Get(typeof(TModel));

                SqlStatement columnNames = this.CreateSqlStatement(),
                    values = this.CreateSqlStatement();
                foreach (var col in tableMap.Columns)
                {
                    var val = col.SourceProperty.GetValue(value);
                    if (val == null ||
                        !col.IsNonNull &&
                        col.SourceProperty.PropertyType.StripNullable() == col.SourceProperty.PropertyType &&
                        (
                        val.Equals(default(Int32)) ||
                        val.Equals(default(Int64)) ||
                        val.Equals(default(Guid)) ||
                        val.Equals(default(DateTime)) ||
                        val.Equals(default(DateTimeOffset)) ||
                        val.Equals(default(Decimal))))
                        val = null;

                    if (col.IsAutoGenerated && val == null)
                    {
                        // Uh-oh, the column is auto-gen, the type of uuid and the engine can't do it!
                        if (col.SourceProperty.PropertyType.StripNullable() == typeof(Guid) &&
                            !this.m_provider.Features.HasFlag(SqlEngineFeatures.AutoGenerateGuids))
                        {
                            val = Guid.NewGuid();
                            col.SourceProperty.SetValue(value, val);
                        }
                        else
                            continue;
                    }

                    columnNames.Append($"{col.Name}");

                    // Append value
                    values.Append("?", val);

                    values.Append(",");
                    columnNames.Append(",");
                }
                values.RemoveLast();
                columnNames.RemoveLast();

                var returnKeys = tableMap.Columns.Where(o => o.IsAutoGenerated);

                // Return arrays
                var stmt = this.m_provider.Returning(
                    this.CreateSqlStatement($"INSERT INTO {tableMap.TableName} (").Append(columnNames).Append(") VALUES (").Append(values).Append(")"),
                    returnKeys.ToArray()
                );

                // Execute
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt))
                    {
                        try
                        {
                            this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }

                            // There are returned keys and we support simple mode returned inserts
                            if (returnKeys.Any() && this.m_provider.Features.HasFlag(SqlEngineFeatures.ReturnedInsertsAsReader))
                            {
                                using (var rdr = dbc.ExecuteReader())
                                    if (rdr.Read())
                                        foreach (var itm in returnKeys)
                                        {
                                            object ov = this.m_provider.ConvertValue(rdr[itm.Name], itm.SourceProperty.PropertyType);
                                            if (ov != null)
                                                itm.SourceProperty.SetValue(value, ov);
                                        }
                            }
                            // There are returned keys and the provider requires an output parameter to hold the keys
                            else if (returnKeys.Any() && this.m_provider.Features.HasFlag(SqlEngineFeatures.ReturnedInsertsAsParms))
                            {
                                // Define output parameters
                                foreach (var rt in returnKeys)
                                {
                                    var parm = dbc.CreateParameter();
                                    parm.ParameterName = rt.Name;
                                    parm.DbType = this.m_provider.MapParameterType(rt.SourceProperty.PropertyType);
                                    parm.Direction = ParameterDirection.Output;
                                    dbc.Parameters.Add(parm);
                                }

                                dbc.ExecuteNonQuery();

                                // Get the parameter values
                                foreach (IDataParameter parm in dbc.Parameters)
                                {
                                    if (parm.Direction != ParameterDirection.Output) continue;

                                    var itm = returnKeys.First(o => o.Name == parm.ParameterName);
                                    object ov = this.m_provider.ConvertValue(parm.Value, itm.SourceProperty.PropertyType);
                                    if (ov != null)
                                        itm.SourceProperty.SetValue(value, ov);
                                }
                            }
                            else // Provider does not support returned keys
                            {
                                dbc.ExecuteNonQuery();
                                // But... the query wants the keys so we have to query them back if the RETURNING clause fields aren't populated in the source object
                                if (returnKeys.Count() > 0 &&
                                    returnKeys.Any(o => o.SourceProperty.GetValue(value) == (o.SourceProperty.PropertyType.IsValueType ? Activator.CreateInstance(o.SourceProperty.PropertyType) : null)))
                                {


                                    var pkcols = tableMap.Columns.Where(o => o.IsPrimaryKey);
                                    var where = new SqlStatement<TModel>(this.m_provider);
                                    foreach (var pk in pkcols)
                                        where.And($"{pk.Name} = ?", pk.SourceProperty.GetValue(value));
                                    stmt = new SqlStatement<TModel>(this.m_provider).SelectFrom().Where(where);

                                    // Create command and exec
                                    using (var dbcSelect = this.m_provider.CreateCommand(this, stmt))
                                    {
                                        try
                                        {
                                            this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                                            if (this.CommandTimeout.HasValue)
                                            {
                                                dbc.CommandTimeout = this.CommandTimeout.Value;
                                            }

                                            using (var rdr = dbcSelect.ExecuteReader())
                                            {
                                                if (rdr.Read())
                                                {
                                                    foreach (var itm in returnKeys)
                                                    {
                                                        object ov = this.m_provider.ConvertValue(rdr[itm.Name], itm.SourceProperty.PropertyType);
                                                        if (ov != null)
                                                            itm.SourceProperty.SetValue(value, ov);
                                                    }
                                                }
                                            }
                                        }
                                        finally
                                        {
                                            this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);
                                        }
                                    }
                                }
                            }
                        }
                        finally
                        {
                            this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                        }

                    }

                    return value;
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "INSERT executed in {0} ms", sw.ElapsedMilliseconds);
            }
#endif
        }


        /// <summary>
        /// Delete from the database
        /// </summary>
        public void DeleteAll<TModel>(SqlStatement whereClause)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var query = this.CreateSqlStatement<TModel>().DeleteFrom().Where(whereClause);
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, query))
                    {
                        try
                        {
                            this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }
                            dbc.ExecuteNonQuery();
                        }
                        finally
                        {
                            this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                        }
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "DELETE executed in {0} ms", sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Delete from the database
        /// </summary>
        public void Delete<TModel>(TModel obj)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif

                var tableMap = TableMapping.Get(typeof(TModel));
                SqlStatement whereClause = this.CreateSqlStatement();
                foreach (var itm in tableMap.PrimaryKey)
                {
                    var itmValue = itm.SourceProperty.GetValue(obj);
                    whereClause.And($"{itm.Name} = ?", itmValue);
                }

                var query = this.CreateSqlStatement<TModel>().DeleteFrom().Where(whereClause);
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, query))
                    {
                        try
                        {
                            this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }
                            dbc.ExecuteNonQuery();
                        }
                        finally
                        {
                            this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                        }
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "DELETE executed in {0} ms", sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Updates the specified object
        /// </summary>
        public TModel Update<TModel>(TModel value)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                // Build the command
                var tableMap = TableMapping.Get(typeof(TModel));
                SqlStatement<TModel> query = this.CreateSqlStatement<TModel>().UpdateSet();
                SqlStatement whereClause = this.CreateSqlStatement();
                int nUpdatedColumns = 0;
                foreach (var itm in tableMap.Columns)
                {
                    var itmValue = itm.SourceProperty.GetValue(value);

                    if (itmValue == null ||
                        !itm.IsNonNull &&
                        itm.SourceProperty.PropertyType.StripNullable() == itm.SourceProperty.PropertyType &&
                        (
                        itmValue.Equals(default(Guid)) && !tableMap.OrmType.IsConstructedGenericType ||
                        itmValue.Equals(default(DateTime)) ||
                        itmValue.Equals(default(DateTimeOffset)) ||
                        itmValue.Equals(default(Decimal))))
                        itmValue = null;

                    // Only update if specified
                    if (itmValue == null &&
                        !itm.SourceSpecified(value))
                        continue;
                    nUpdatedColumns++;
                    query.Append($"{itm.Name} = ? ", itmValue ?? DBNull.Value);
                    query.Append(",");
                    if (itm.IsPrimaryKey)
                        whereClause.And($"{itm.Name} = ?", itmValue);
                }

                // Nothing being updated
                if (nUpdatedColumns == 0)
                {
                    m_tracer.TraceInfo("Nothing to update, will skip");
                    return value;
                }

                query.RemoveLast();
                query.Where(whereClause);

                // Now update
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, query))
                    {
                        try
                        {
                            this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                            if (this.CommandTimeout.HasValue)
                            {
                                dbc.CommandTimeout = this.CommandTimeout.Value;
                            }
                            dbc.ExecuteNonQuery();
                        }
                        finally
                        {
                            this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                        }
                    }
                }

                return value;
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(EventLevel.Verbose, "UPDATE executed in {0} ms", sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Updates the specified object
        /// </summary>
        public void UpdateAll<TModel>(Expression<Func<TModel, bool>> whereExpression, params Expression<Func<TModel, dynamic>>[] updateStatements)
        {
            this.UpdateAll<TModel, TModel>(whereExpression, updateStatements);
        }

        /// <summary>
        /// Update all methods
        /// </summary>
        /// <typeparam name="TModel">The type of object to update</typeparam>
        /// <typeparam name="TUpdateModel">The type that update statements should be treated as</typeparam>
        /// <param name="whereExpression">The filter expression</param>
        /// <param name="updateStatements">The update statements to append to the SQL clause</param>
        public void UpdateAll<TModel, TUpdateModel>(Expression<Func<TModel, bool>> whereExpression, params Expression<Func<TUpdateModel, dynamic>>[] updateStatements)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                // Is TModel assignable from TUpdateMode
                if (typeof(TModel) != typeof(TUpdateModel) && !typeof(TUpdateModel).IsAssignableFrom(typeof(TModel)))
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(TUpdateModel), typeof(TModel)));
                }

                // Build the command
                var tableMap = TableMapping.Get(typeof(TModel));
                var updateStatement = this.CreateSqlStatement<TModel>().UpdateSet() as SqlStatement;
                var whereClause = this.CreateSqlStatement().Where(whereExpression);
                var setClause = this.CreateSqlStatement();
                foreach (var updateFunc in updateStatements)
                {
                    var queryBuilder = new SqlQueryExpressionBuilder(tableMap.TableName, this.m_provider);
                    queryBuilder.Visit(updateFunc);
                    setClause.Append(queryBuilder.SqlStatement);
                    setClause.Append(",");
                }
                setClause.RemoveLast();
                updateStatement = updateStatement.Append(setClause.Build()).Append(whereClause.Build());

                // Now update
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, updateStatement))
                    {
                        dbc.ExecuteNonQuery();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "UPDATE executed in {0} ms", sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Delete from the database
        /// </summary>
        public void DeleteAll<TModel>(Expression<Func<TModel, bool>> where)
        {
            var tableMap = TableMapping.Get(typeof(TModel));
            var queryBuilder = new SqlQueryExpressionBuilder(tableMap.TableName, this.m_provider);
            queryBuilder.Visit(where.Body);
            this.DeleteAll<TModel>(queryBuilder.SqlStatement);

        }

        /// <summary>
        /// Execute a non query
        /// </summary>
        public void ExecuteNonQuery(SqlStatement stmt)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject)
                {
                    var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
                        this.IncrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                        if (this.CommandTimeout.HasValue)
                        {
                            dbc.CommandTimeout = this.CommandTimeout.Value;
                        }

                        dbc.ExecuteNonQuery();
                    }
                    finally
                    {
                        dbc.Dispose();

                        this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                this.m_tracer.TraceEvent(EventLevel.Verbose, "EXECUTE NON QUERY executed in {0} ms", sw.ElapsedMilliseconds);
            }
#endif
        }

    }
}