/*
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
 * Date: 2023-6-21
 */
using SanteDB.Core.Diagnostics.Performance;
using SanteDB.OrmLite.Configuration;
using SanteDB.OrmLite.Providers;
using SanteDB.OrmLite.Providers.Sqlite;
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
            typeof(long),
            typeof(long?),
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
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);
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
            var sqlStatement = this.CreateSqlStatementBuilder();
            foreach (var cm in TableMapping.Get(typeof(TModel)).Columns.Where(o => o.IsPrimaryKey))
            {
                sqlStatement.And($"{cm.Name} = ?", cm.SourceProperty.GetValue(obj));
            }

            sqlStatement = this.CreateSqlStatementBuilder().SelectFrom(typeof(TModel), ColumnMapping.One).Where(sqlStatement.Statement);
            return this.Any(sqlStatement.Statement);
        }

        /// <summary>
        /// Determines if <paramref name="modelKey"/> of type <paramref name="type"/> exists in the database
        /// </summary>
        public bool Exists(Type type, Guid modelKey)
        {
            var sqlStatement = this.CreateSqlStatementBuilder();
            foreach (var cm in TableMapping.Get(type).Columns.Where(o => o.IsPrimaryKey))
            {
                sqlStatement.And($"{cm.Name} = ?", modelKey);
            }

            sqlStatement = this.CreateSqlStatementBuilder().SelectFrom(type, ColumnMapping.One).Where(sqlStatement.Statement);
            return this.Any(sqlStatement.Statement);
        }

        /// <summary>
        /// Map an object
        /// </summary>
        private TModel MapObject<TModel>(IDataReader rdr)
        {
            if (typeof(Object) == typeof(TModel))
            {
                return default(TModel);
            }
            else if (typeof(CompositeResult).IsAssignableFrom(typeof(TModel)))
            {
                var retVal = Activator.CreateInstance(typeof(TModel));
                (retVal as CompositeResult).ParseValues(rdr, this.m_provider);
                return (TModel)retVal;
            }
            else if (BaseTypes.Contains(typeof(TModel)))
            {
                var obj = rdr[0];
                if (obj == DBNull.Value)
                {
                    return default(TModel);
                }
                else if (typeof(TModel).IsAssignableFrom(obj.GetType()))
                {
                    return (TModel)obj;
                }
                else
                {
                    return (TModel)this.m_provider.ConvertValue(obj, typeof(TModel));
                }
            }
            else if (typeof(Object) == typeof(TModel)) // Any old object
            {
                return (TModel)this.m_provider.ConvertValue(rdr[0], typeof(TModel));
            }
            else if (typeof(ExpandoObject).IsAssignableFrom(typeof(TModel)))
            {
                return this.MapExpando<TModel>(rdr);
            }
            else
            {
                return (TModel)this.MapObject(typeof(TModel), rdr);
            }
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
                var type = rdr.GetFieldType(i);
                var name = rdr.GetName(i).ToLowerInvariant();
                if (name.EndsWith("_utc") || name.EndsWith("_time")) // HACK: For sqlite
                {
                    type = typeof(DateTime);
                }

                if (value == DBNull.Value)
                {
                    value = null;
                }
                else if (this.m_encryptionProvider?.HasEncryptionMagic(value) == true && this.m_encryptionProvider.TryDecrypt(value, out var decrypted))
                {
                    value = this.m_provider.ConvertValue(decrypted, type);
                }
                else
                {
                    value = this.m_provider.ConvertValue(value, type);
                }

                if (!retVal.ContainsKey(name)) // Overwrite duplicate named results
                {
                    retVal.Add(name, value);
                }
                else
                {
                    m_tracer.TraceWarning("Tuple does not have unique column names");
                    retVal[name] = value;
                }
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
                    object dbValue = rdr[itm.Name];
                    _ = this.m_encryptionProvider?.TryGetEncryptionMode(itm.EncryptedColumnId, out _) == true &&
                        this.m_encryptionProvider?.TryDecrypt(dbValue, out dbValue) == true;

                    object value = this.m_provider.ConvertValue(dbValue, itm.SourceProperty.PropertyType);
                    if (!itm.IsSecret)
                    {
                        // Hack for SQLite - 
                        if (itm.SourceProperty.PropertyType.StripNullable() == typeof(byte[]) && value is Guid gval)
                        {
                            value = gval.ToByteArray();
                        }
                        itm.SourceProperty.SetValue(result, value);
                    }
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
                                return this.ReaderToResult(returnType, rdr);
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);


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
                            {
                                return this.ReaderToResult<TModel>(rdr);
                            }
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
                var builder = this.CreateSqlStatementBuilder().SelectFrom(typeof(TModel)).Where(querySpec).Limit(1);
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, builder.Statement))
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
                                return this.ReaderToResult<TModel>(rdr);
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
                                return this.ReaderToResult<TModel>(rdr);
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
                var stmt = this.CreateSqlStatementBuilder().SelectFrom(typeof(TModel)).Where(querySpec).Limit(2).Statement;

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
                                if (!rdr.Read())
                                {
                                    return retVal;
                                }
                                else
                                {
                                    throw new InvalidOperationException("Sequence contains more than one element");
                                }
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
                var stmt = this.CreateSqlStatementBuilder().SelectFrom(typeof(TModel), ColumnMapping.One).Where(querySpec).Statement;
                stmt = this.m_provider.StatementFactory.Exists(stmt);
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
                            return this.Provider.ConvertValue<bool>(dbc.ExecuteScalar());
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
                var stmt = this.m_provider.StatementFactory.Exists(querySpec);
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

                            return this.m_provider.ConvertValue<bool>(dbc.ExecuteScalar());
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
                var stmt = this.CreateSqlStatementBuilder().SelectFrom(typeof(TModel)).Where(querySpec).Statement;
                stmt = this.m_provider.StatementFactory.Count(stmt);
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
                var stmt = this.m_provider.StatementFactory.Count(querySpec);
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

            }
#endif
        }

        /// <summary>
        /// Parse to a single result
        /// </summary>
        private TModel ReaderToResult<TModel>(IDataReader rdr)
        {
            if (rdr.Read())
            {
                return this.MapObject<TModel>(rdr);
            }
            else
            {
                return default(TModel);
            }
        }

        /// <summary>
        /// Parse to a single result
        /// </summary>
        private object ReaderToResult(Type returnType, IDataReader rdr)
        {
            if (rdr.Read())
            {
                return this.MapObject(returnType, rdr);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Execute the specified query
        /// </summary>
        public OrmResultSet<TModel> Query<TModel>(Expression<Func<TModel, bool>> querySpec)
        {
            this.ThrowIfDisposed();
            var stmt = this.CreateSqlStatementBuilder().SelectFrom(typeof(TModel)).Where(querySpec).Statement;
            return new OrmResultSet<TModel>(this, stmt);
        }

        /// <summary>
        /// Query using the specified statement
        /// </summary>
        public OrmResultSet<TModel> Query<TModel>(SqlStatement query)
        {
            this.ThrowIfDisposed();
            return new OrmResultSet<TModel>(this, query);
        }

        /// <summary>
        /// Non-generic implementation for query
        /// </summary>
        public IOrmResultSet Query(Type modelType, SqlStatement query)
        {
            this.ThrowIfDisposed();
            var ormType = typeof(OrmResultSet<>).MakeGenericType(modelType);
            var ctor = ormType.GetConstructors(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).First();
            return ctor.Invoke(new object[] { this, query }) as IOrmResultSet;
        }

        /// <summary>
        /// Executes the query against the database
        /// </summary>
        public IEnumerable<TModel> ExecQuery<TModel>(SqlStatement query)
        {
            this.ThrowIfDisposed();
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
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
            this.ThrowIfDisposed();

#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                // First we want to map object to columns
                var tableMap = TableMapping.Get(typeof(TModel));

                SqlStatementBuilder columnNames = this.CreateSqlStatementBuilder(),
                    values = this.CreateSqlStatementBuilder();
                foreach (var col in tableMap.Columns)
                {
                    var val = col.SourceProperty.GetValue(value);
                    bool valIsDefault = val != null && col.SourceProperty.PropertyType.StripNullable() == col.SourceProperty.PropertyType &&
                        (
                        val.Equals(default(Int32)) ||
                        val.Equals(default(Int64)) ||
                        val.Equals(default(Guid)) ||
                        val.Equals(default(DateTime)) ||
                        val.Equals(default(DateTimeOffset)) ||
                        val.Equals(default(Decimal))
                        );

                    if (val == null ||
                        !col.IsNonNull && valIsDefault)
                    {
                        val = col.DefaultValue;
                    }

                    if (col.IsAutoGenerated && (val == null || valIsDefault))
                    {
                        // Uh-oh, the column is auto-gen, the type of uuid and the engine can't do it!
                        if (col.SourceProperty.PropertyType.StripNullable() == typeof(Guid) &&
                            !this.m_provider.StatementFactory.Features.HasFlag(SqlEngineFeatures.AutoGenerateGuids))
                        {
                            val = Guid.NewGuid();
                            col.SourceProperty.SetValue(value, val);
                        }
                        else if ((col.SourceProperty.PropertyType.StripNullable() == typeof(long) ||
                            col.SourceProperty.PropertyType.StripNullable() == typeof(int)) &&
                            !this.m_provider.StatementFactory.Features.HasFlag(SqlEngineFeatures.AutoGenerateSequences))
                        {
                            columnNames.Append($"{col.Name}").Append(",");
                            values.Append("(").Append(this.Provider.StatementFactory.GetNextSequenceValue(tableMap.TableName)).Append(")").Append(",");
                            continue;
                        }
                        else
                        {
                            continue;
                        }
                    }

                    columnNames.Append($"{col.Name}");

                    // Encrypt value
                    OrmAleMode aleMode = OrmAleMode.Off;
                    _ = this.m_encryptionProvider?.TryGetEncryptionMode(col.EncryptedColumnId, out aleMode) == true &&
                        this.m_encryptionProvider?.TryEncrypt(aleMode, val, out val) == true;

                    // Append value
                    values.Append("?", val);

                    values.Append(",");
                    columnNames.Append(",");
                }
                values.RemoveLast(out _);
                columnNames.RemoveLast(out _);

                var returnKeys = tableMap.Columns.Where(o => o.IsAutoGenerated);

                // Return arrays
                var stmt = $"INSERT INTO {tableMap.TableName} (" + columnNames.Statement + ") VALUES (" + values.Statement + ") "
                    + this.m_provider.StatementFactory.Returning(returnKeys.ToArray());

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
                            if (returnKeys.Any() && this.m_provider.StatementFactory.Features.HasFlag(SqlEngineFeatures.ReturnedInsertsAsReader))
                            {
                                using (var rdr = dbc.ExecuteReader())
                                {
                                    if (rdr.Read())
                                    {
                                        foreach (var itm in returnKeys)
                                        {
                                            object ov = this.m_provider.ConvertValue(rdr[itm.Name], itm.SourceProperty.PropertyType);
                                            if (ov != null)
                                            {
                                                itm.SourceProperty.SetValue(value, ov);
                                            }
                                        }
                                    }
                                }
                            }
                            // There are returned keys and the provider requires an output parameter to hold the keys
                            else if (returnKeys.Any() && this.m_provider.StatementFactory.Features.HasFlag(SqlEngineFeatures.ReturnedInsertsAsParms))
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

                                var retV = dbc.ExecuteNonQuery();

                                // Get the parameter values
                                foreach (IDataParameter parm in dbc.Parameters)
                                {
                                    if (parm.Direction != ParameterDirection.Output)
                                    {
                                        continue;
                                    }

                                    var itm = returnKeys.First(o => o.Name == parm.ParameterName);
                                    object ov = this.m_provider.ConvertValue(parm.Value, itm.SourceProperty.PropertyType);
                                    if (ov != null)
                                    {
                                        itm.SourceProperty.SetValue(value, ov);
                                    }
                                }
                            }
                            else // Provider does not support returned keys
                            {
                                var retV = dbc.ExecuteNonQuery();
                                // But... the query wants the keys so we have to query them back if the RETURNING clause fields aren't populated in the source object
                                if (returnKeys.Count() > 0 &&
                                    returnKeys.Any(o => o.SourceProperty.GetValue(value) == (o.SourceProperty.PropertyType.IsValueType ? Activator.CreateInstance(o.SourceProperty.PropertyType) : null)))
                                {


                                    var pkcols = tableMap.Columns.Where(o => o.IsPrimaryKey);
                                    var where = new SqlStatementBuilder(this.m_provider.StatementFactory);
                                    foreach (var pk in pkcols)
                                    {
                                        where.And($"{pk.Name} = ?", pk.SourceProperty.GetValue(value));
                                    }

                                    stmt = new SqlStatementBuilder(this.m_provider.StatementFactory).SelectFrom(typeof(TModel)).Where(where.Statement).Statement;

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
                                                        {
                                                            itm.SourceProperty.SetValue(value, ov);
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
                                }
                            }
                        }
                        finally
                        {
                            this.DecrementProbe(Diagnostics.OrmPerformanceMetric.ActiveStatements);

                        }

                    }

#if DEBUG
                    // SQLITE Sometimes "says" it inserted data (i.e. the connector does not fail) however it doesn't actually insert
                    // the data - (facepalm)
                    // SQLITE Sucks so bad 
                    if (this.m_provider.Invariant == SqliteProvider.InvariantName && !this.Exists(value))
                    {
                        Debug.WriteLine("SQLITE MESSED UP: {0}", value);
                    }
#endif 
                    return value;
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.AddProbeResponseTime(sw.ElapsedMilliseconds);
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);


            }
#endif
        }


        /// <summary>
        /// Delete from the database
        /// </summary>
        public void DeleteAll(Type tmodel, SqlStatement whereClause)
        {
            this.ThrowIfDisposed();

#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var query = this.CreateSqlStatementBuilder().DeleteFrom(tmodel).Where(whereClause).Statement;
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

            }
#endif
        }

        /// <summary>
        /// Delete from the database
        /// </summary>
        public void Delete<TModel>(TModel obj)
        {
            this.ThrowIfDisposed();

#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif

                var tableMap = TableMapping.Get(obj.GetType());
                SqlStatementBuilder whereClauseBuilder = this.CreateSqlStatementBuilder();
                foreach (var itm in tableMap.PrimaryKey)
                {
                    var itmValue = itm.SourceProperty.GetValue(obj);
                    whereClauseBuilder.And($"{itm.Name} = ?", itmValue);
                }

                var query = this.CreateSqlStatementBuilder().DeleteFrom(obj.GetType()).Where(whereClauseBuilder.Statement).Statement;
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

            }
#endif
        }

        /// <summary>
        /// Updates the specified object
        /// </summary>
        public TModel Update<TModel>(TModel value)
        {
            this.ThrowIfDisposed();

#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                // Build the command
                var tableMap = TableMapping.Get(value.GetType());
                SqlStatementBuilder queryBuilder = this.CreateSqlStatementBuilder().UpdateSet(value.GetType());
                SqlStatementBuilder whereClauseBuilder = this.CreateSqlStatementBuilder();
                int nUpdatedColumns = 0;
                foreach (var col in tableMap.Columns)
                {
                    var itmValue = col.SourceProperty.GetValue(value);

                    if (itmValue == null ||
                        !col.IsNonNull &&
                        col.SourceProperty.PropertyType.StripNullable() == col.SourceProperty.PropertyType &&
                        (
                        itmValue.Equals(default(Guid)) && !tableMap.OrmType.IsConstructedGenericType ||
                        itmValue.Equals(default(DateTime)) ||
                        itmValue.Equals(default(DateTimeOffset)) ||
                        itmValue.Equals(default(Decimal))))
                    {
                        itmValue = null;
                    }

                    // Only update if specified
                    if (itmValue == null &&
                        !col.SourceSpecified(value))
                    {
                        continue;
                    }

                    // Encrypt value
                    OrmAleMode aleMode = OrmAleMode.Off;
                    _ = this.m_encryptionProvider?.TryGetEncryptionMode(col.EncryptedColumnId, out aleMode) == true &&
                        this.m_encryptionProvider?.TryEncrypt(aleMode, itmValue, out itmValue) == true;

                    nUpdatedColumns++;
                    queryBuilder.Append($"{col.Name} = ? ", itmValue ?? DBNull.Value);
                    queryBuilder.Append(",");
                    if (col.IsPrimaryKey)
                    {
                        whereClauseBuilder.And($"{col.Name} = ?", itmValue);
                    }
                }

                // Nothing being updated
                if (nUpdatedColumns == 0)
                {
                    m_tracer.TraceInfo("Nothing to update, will skip");
                    return value;
                }

                queryBuilder.RemoveLast(out _).Where(whereClauseBuilder.Statement);

                // Now update
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, queryBuilder.Statement))
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

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
        /// Update all data matching <paramref name="whereExpression"/> to <paramref name="updateStatements"/>
        /// </summary>
        /// <typeparam name="TModel">The type of object to update</typeparam>
        /// <typeparam name="TUpdateModel">The type that update statements should be treated as</typeparam>
        /// <param name="whereExpression">The filter expression</param>
        /// <param name="updateStatements">The update statements to append to the SQL clause</param>
        public void UpdateAll<TModel, TUpdateModel>(Expression<Func<TModel, bool>> whereExpression, params Expression<Func<TUpdateModel, dynamic>>[] updateStatements)
        {
            this.UpdateAll(typeof(TModel), whereExpression, updateStatements);
        }

        /// <summary>
        /// Update all 
        /// </summary>
        public void UpdateAll(Type tmodel, LambdaExpression whereExpression, params LambdaExpression[] updateStatements)
        {

            // Convert where clause
            var tableMap = TableMapping.Get(tmodel);
            var queryBuilder = new SqlQueryExpressionBuilder(tableMap.TableName, this.m_provider.StatementFactory);
            queryBuilder.Visit(whereExpression.Body);

            this.UpdateAll(tmodel, queryBuilder.StatementBuilder.Statement, updateStatements);
        }

        /// <summary>
        /// Update all with specified Sql based statement
        /// </summary>
        public void UpdateAll<TModel>(SqlStatement whereExpression, params Expression<Func<TModel, dynamic>>[] updateStatements)
        {

            if (whereExpression.Contains("SELECT"))
            {
                whereExpression = whereExpression.Prepare();
                var match = Constants.ExtractRawSqlStatementRegex.Match(whereExpression.Sql);
                var where = match.Groups[Constants.SQL_GROUP_WHERE].Value;
                whereExpression = new SqlStatement(where, whereExpression.Arguments);
            }
            this.UpdateAll(typeof(TModel), whereExpression, updateStatements);
        }

        /// <summary>
        /// Update all data with specified where clause
        /// </summary>
        public void UpdateAll(Type tmodel, SqlStatement whereClause, params LambdaExpression[] updateStatements)
        {
            this.ThrowIfDisposed();

#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif

                // Build the command
                var tableMap = TableMapping.Get(tmodel);
                var updateStatementBuilder = this.CreateSqlStatementBuilder().UpdateSet(tmodel);
                var setClause = SqlStatement.Empty;
                foreach (var updateFunc in updateStatements)
                {
                    var queryBuilder = new SqlQueryExpressionBuilder(tableMap.TableName, this.m_provider.StatementFactory, false, nullAsIs: false);
                    queryBuilder.Visit(updateFunc);
                    setClause += queryBuilder.StatementBuilder.Statement + ",";
                }

                updateStatementBuilder.Append(setClause.RemoveLast(out _))
                    .Where(whereClause);

                // Now update
                lock (this.m_lockObject)
                {
                    using (var dbc = this.m_lastCommand = this.m_provider.CreateCommand(this, updateStatementBuilder.Statement))
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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

            }
#endif
        }

        /// <summary>
        /// Delete from the database
        /// </summary>
        public void DeleteAll<TModel>(Expression<Func<TModel, bool>> where) => this.DeleteAll(typeof(TModel), where);

        /// <summary>
        /// Update all 
        /// </summary>
        public void DeleteAll(Type tmodel, LambdaExpression whereExpression)
        {
            // Convert where clause
            var tableMap = TableMapping.Get(tmodel);
            var queryBuilder = new SqlQueryExpressionBuilder(tableMap.TableName, this.m_provider.StatementFactory);
            queryBuilder.Visit(whereExpression.Body);

            this.DeleteAll(tmodel, queryBuilder.StatementBuilder.Statement);
        }

        /// <summary>
        /// Update all with specified Sql based statement
        /// </summary>
        public void DeleteAll<TModel>(SqlStatement whereExpression)
        {
            if (whereExpression.Contains("SELECT"))
            {
                whereExpression = whereExpression.Prepare();
                var match = Constants.ExtractRawSqlStatementRegex.Match(whereExpression.Sql);
                var where = match.Groups[Constants.SQL_GROUP_WHERE].Value;
                whereExpression = new SqlStatement(where, whereExpression.Arguments);
            }
            this.DeleteAll(typeof(TModel), whereExpression);
        }

        /// <summary>
        /// Execute the specified SQL
        /// </summary>
        public void ExecuteNonQuery(String sql, params object[] args)
        {
            this.ExecuteNonQuery(new SqlStatement(sql, args));
        }

        /// <summary>
        /// Execute a non query
        /// </summary>
        public void ExecuteNonQuery(SqlStatement stmt)
        {
            this.ThrowIfDisposed();

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
                PerformanceTracer.WritePerformanceTrace(sw.ElapsedMilliseconds);

            }
#endif
        }

        /// <summary>
        /// Create a table
        /// </summary>
        public void CreateTable<TTable>()
        {
            this.ThrowIfDisposed();

            var statement = this.CreateSqlStatementBuilder();
            var tableMap = TableMapping.Get(typeof(TTable));

            statement.Append($"CREATE TABLE {tableMap.TableName} (");
            foreach (var col in tableMap.Columns)
            {
                statement.Append($"{col.Name} {this.Provider.MapSchemaDataType(col.SourceProperty.PropertyType)} ");
                if (col.IsAutoGenerated)
                {
                    if (col.SourceProperty.PropertyType.StripNullable() == typeof(Guid))
                    {
                        statement.Append(this.Provider.StatementFactory.CreateSqlKeyword(SqlKeyword.NewGuid));
                    }
                    else if (col.SourceProperty.PropertyType.StripNullable() == typeof(DateTime) ||
                        col.SourceProperty.PropertyType.StripNullable() == typeof(DateTimeOffset))
                    {
                        statement.Append(this.Provider.StatementFactory.CreateSqlKeyword(SqlKeyword.CurrentTimestamp));
                    }
                    else
                    {
                        throw new NotSupportedException(col.ToString());
                    }
                }
                if (col.IsNonNull)
                {
                    statement.Append(" NOT NULL ");
                }
                if (col.IsUnique)
                {
                    statement.Append(" UNIQUE ");
                }
                statement.Append(", ");
            }

            // Append primary key constraint
            statement.Append($"CONSTRAINT PK_{tableMap.TableName} PRIMARY KEY ({String.Join(",", tableMap.PrimaryKey.Select(o => o.Name))})").Append(",");
            foreach (var col in tableMap.Columns.Where(c => c.ForeignKey != null))
            {
                var otherTable = TableMapping.Get(col.ForeignKey.Table);
                statement.Append($"CONSTRAINT FK_{tableMap.TableName}_{col.Name} FOREIGN KEY ({col.Name}) REFERENCES {otherTable.TableName}({otherTable.GetColumn(col.ForeignKey.Column).Name})").Append(",");
            }
            statement.RemoveLast(out _).Append(")");
            this.ExecuteNonQuery(statement.Statement.Prepare());

        }

        /// <summary>
        /// Create a table
        /// </summary>
        public void DropTable<TTable>()
        {
            this.ThrowIfDisposed();

            var statement = this.CreateSqlStatementBuilder();
            var tableMap = TableMapping.Get(typeof(TTable));

            statement.Append($"DROP TABLE {tableMap.TableName};");
            this.ExecuteNonQuery(statement.Statement.Prepare());

        }

    }
}