﻿/*
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
 * User: fyfej
 * Date: 2017-9-1
 */
using SanteDB.Core.Model.Map;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.IO;
using SanteDB.Core.Model;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Multi type result used when a result set is a join
    /// </summary>
    public abstract class CompositeResult
    {

        /// <summary>
        /// Gets or sets the values
        /// </summary>
        public Object[] Values { get; protected set; }

        // Parse values
        public abstract void ParseValues(IDataReader rdr, IDbProvider provider);

        /// <summary>
        /// Parse the data
        /// </summary>
        protected TData Parse<TData>(IDataReader rdr, IDbProvider provider)
        {
            var tableMapping = TableMapping.Get(typeof(TData));
            dynamic result = Activator.CreateInstance(typeof(TData));
            // Read each column and pull from reader
            foreach (var itm in tableMapping.Columns)
            {
                try
                {
                    object value = provider.ConvertValue(rdr[itm.Name], itm.SourceProperty.PropertyType);
                    itm.SourceProperty.SetValue(result, value);
                }
                catch
                {
                   throw new MissingFieldException(tableMapping.TableName, itm.Name);
                }
            }
            return result;
        }
    }


    /// <summary>
    /// Multi-type result for two types
    /// </summary>
    public class CompositeResult<TData1, TData2> : CompositeResult
    {

        public TData1 Object1 { get { return (TData1)this.Values[0]; } }
        public TData2 Object2 { get { return (TData2)this.Values[1]; } }

        public override void ParseValues(IDataReader rdr, IDbProvider provider)
        {
            this.Values = new object[] { this.Parse<TData1>(rdr, provider), this.Parse<TData2>(rdr, provider) };
        }
    }

    /// <summary>
    /// Multi-type result for three types
    /// </summary>
    public class CompositeResult<TData1, TData2, TData3> : CompositeResult<TData1, TData2>
    {
        public TData3 Object3 { get { return (TData3)this.Values[2]; } }

        public override void ParseValues(IDataReader rdr, IDbProvider provider)
        {
            this.Values = new object[] { this.Parse<TData1>(rdr, provider), this.Parse<TData2>(rdr, provider), this.Parse<TData3>(rdr, provider) };
        }
    }

    /// <summary>
    /// Multi-type result for four types
    /// </summary>
    public class CompositeResult<TData1, TData2, TData3, TData4> : CompositeResult<TData1, TData2, TData3>
    {
        public TData4 Object4 { get { return (TData4)this.Values[3]; } }

        public override void ParseValues(IDataReader rdr, IDbProvider provider)
        {
            this.Values = new object[] { this.Parse<TData1>(rdr, provider), this.Parse<TData2>(rdr, provider), this.Parse<TData3>(rdr, provider), this.Parse<TData4>(rdr, provider) };
        }
    }

    /// <summary>
    /// Data context functions for the execution of query data
    /// </summary>
    public partial class DataContext
    {

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
                lock (this.m_lockObject)
                {
                    var dbc = this.m_provider.CreateStoredProcedureCommand(this, spName, arguments);
                    try
                    {
                        using (var rdr = dbc.ExecuteReader())
                            return this.ReaderToCollection<TModel>(rdr).ToList();
                    }
                    catch (TimeoutException)
                    {
                        try { dbc.Cancel(); } catch { }
                        throw;
                    }
                    finally
                    {
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }
#if DEBUG 
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "QUERY {0} executed in {1} ms", spName, sw.ElapsedMilliseconds);
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
        /// Reader to collection of objects
        /// </summary>
        private IEnumerable<TModel> ReaderToCollection<TModel>(IDataReader rdr)
        {
            while (rdr.Read())
                yield return this.MapObject<TModel>(rdr);
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
                foreach (var itm in (retVal as CompositeResult).Values.OfType<IAdoLoadedData>())
                    itm.Context = this;
                return (TModel)retVal;
            }
            else if (BaseTypes.Contains(typeof(TModel)))
                return (TModel)rdr[0];
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
                var name = rdr.GetName(i);
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
            dynamic result = Activator.CreateInstance(tModel);
            // Read each column and pull from reader
            foreach (var itm in tableMapping.Columns)
            {
                try
                {
                    object value = this.m_provider.ConvertValue(rdr[itm.Name], itm.SourceProperty.PropertyType);
                    itm.SourceProperty.SetValue(result, value);
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceEvent(TraceEventType.Error, 0, "Error mapping: {0} : {1}", itm.Name, e.ToString());
                    throw new MissingFieldException(tableMapping.TableName, itm.Name);
                }
            }

            if (result is IAdoLoadedData)
                (result as IAdoLoadedData).Context = this;
            else
                this.m_tracer.TraceEvent(TraceEventType.Information, 0, "Type {0} does not implement IAdoLoadedData", tModel);
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
                    var dbc = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
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
#if DBPERF
                        this.PerformanceMonitor(stmt, sw);
#endif
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "QUERY {0} executed in {1} ms", this.GetQueryLiteral(stmt), sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// First or default returns only the first object or null if not found
        /// </summary>
        public TModel FirstOrDefault<TModel>(String spName, params object[] arguments)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject)
                {
                    var dbc = this.m_provider.CreateStoredProcedureCommand(this, spName, arguments);
                    try
                    {
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
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "FIRST {0} executed in {1} ms", spName, sw.ElapsedMilliseconds);
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
                    var dbc = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
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

#if DBPERF
                        this.PerformanceMonitor(stmt, sw);
#endif
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "FIRST {0} executed in {1} ms", querySpec, sw.ElapsedMilliseconds);
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
                    var dbc = this.m_provider.CreateCommand(this, stmt.Limit(1));
                    try
                    {
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
#if DBPERF
                        this.PerformanceMonitor(stmt, sw);
#endif
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "FIRST {0} executed in {1} ms", this.GetQueryLiteral(stmt), sw.ElapsedMilliseconds);
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
                    var dbc = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
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
#if DBPERF
                        this.PerformanceMonitor(stmt, sw);
#endif
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "SINGLE {0} executed in {1} ms", querySpec, sw.ElapsedMilliseconds);
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
                var stmt = this.m_provider.Exists(this.CreateSqlStatement<TModel>().SelectFrom().Where(querySpec));
                lock (this.m_lockObject)
                {
                    var dbc = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
                        return (bool)dbc.ExecuteScalar();
                    }
                    catch (TimeoutException)
                    {
                        try { dbc.Cancel(); } catch { }
                        throw;
                    }
                    finally
                    {
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "ANY {0} executed in {1} ms", querySpec, sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Represents the count function
        /// </summary>
        internal bool Any(SqlStatement querySpec)
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
                    var dbc = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
                        return (bool)dbc.ExecuteScalar();
                    }
                    catch (TimeoutException)
                    {
                        try { dbc.Cancel(); } catch { }
                        throw;
                    }
                    finally
                    {
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }


#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "ANY {0} executed in {1} ms", this.GetQueryLiteral(querySpec), sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Returns only if only one result is available
        /// </summary>
        public int Count<TModel>(Expression<Func<TModel, bool>> querySpec)
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
                    var dbc = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
                        return (int)dbc.ExecuteScalar();
                    }
                    catch (TimeoutException)
                    {
                        try { dbc.Cancel(); } catch { }
                        throw;
                    }
                    finally
                    {
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "COUNT {0} executed in {1} ms", querySpec, sw.ElapsedMilliseconds);
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
                    var dbc = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
                        return Convert.ToInt32(dbc.ExecuteScalar());
                    }
                    catch (TimeoutException)
                    {
                        try { dbc.Cancel(); } catch { }
                        throw;
                    }
                    finally
                    {
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "COUNT {0} executed in {1} ms", this.GetQueryLiteral(querySpec), sw.ElapsedMilliseconds);
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
        public IEnumerable<TModel> Query<TModel>(Expression<Func<TModel, bool>> querySpec)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var query = this.CreateSqlStatement<TModel>().SelectFrom().Where(querySpec);
                lock (this.m_lockObject)
                {
                    var dbc = this.m_provider.CreateCommand(this, query);
                    try
                    {
                        using (var rdr = dbc.ExecuteReader())
                            return this.ReaderToCollection<TModel>(rdr).ToList();
                    }
                    catch (TimeoutException)
                    {
                        try { dbc.Cancel(); } catch { }
                        throw;
                    }
                    finally
                    {
#if DBPERF
                        this.PerformanceMonitor(query, sw);
#endif
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "QUERY {0} executed in {1} ms", querySpec, sw.ElapsedMilliseconds);
            }
#endif
        }

        /// <summary>
        /// Adds data in a safe way
        /// </summary>
        public void AddData(string key, object value)
        {
            lock (this.m_dataDictionary)
                if (!this.m_dataDictionary.ContainsKey(key))
                    this.m_dataDictionary.Add(key, value);
        }

        /// <summary>
        /// Query using the specified statement
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public IEnumerable<TModel> Query<TModel>(SqlStatement query)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                lock (this.m_lockObject)
                {
                    var dbc = this.m_provider.CreateCommand(this, query);
                    try
                    {
                        using (var rdr = dbc.ExecuteReader())
                            return this.ReaderToCollection<TModel>(rdr).ToList();

                    }
                    catch (TimeoutException)
                    {
                        try { dbc.Cancel(); } catch { }
                        throw;
                    }
                    finally
                    {
#if DBPERF
                        this.PerformanceMonitor(query, sw);
#endif
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "QUERY {0} executed in {1} ms", this.GetQueryLiteral(query), sw.ElapsedMilliseconds);
            }
#endif
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
                        !col.IsNonNull && (
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
                    else if (col.IsHashed && val != null)
                        using (var sha = System.Security.Cryptography.SHA256.Create())
                        {
                            if (val is byte[])
                                val = sha.ComputeHash(val as byte[]);
                            else if (val is String)
                                val = BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(val.ToString()))).Replace("-","");
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
                    var dbc = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
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
                                if (!this.IsPreparedCommand(dbc))
                                    dbc.Dispose();

                                var pkcols = tableMap.Columns.Where(o => o.IsPrimaryKey);
                                var where = new SqlStatement<TModel>(this.m_provider);
                                foreach (var pk in pkcols)
                                    where.And($"{pk.Name} = ?", pk.SourceProperty.GetValue(value));
                                stmt = new SqlStatement<TModel>(this.m_provider).SelectFrom().Where(where);

                                // Create command and exec
                                dbc = this.m_provider.CreateCommand(this, stmt);
                                using (var rdr = dbc.ExecuteReader())
                                    if (rdr.Read())
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
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

                if (value is IAdoLoadedData)
                    (value as IAdoLoadedData).Context = this;

                return value;
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "INSERT executed in {0} ms", sw.ElapsedMilliseconds);
            }
#endif

        }

        /// <summary>
        /// Delete from the database
        /// </summary>
        public void Delete<TModel>(Expression<Func<TModel, bool>> where)
        {
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
            try
            {
#endif
                var query = this.CreateSqlStatement<TModel>().DeleteFrom().Where(where);
                lock (this.m_lockObject)
                {
                    var dbc = this.m_provider.CreateCommand(this, query);
                    try
                    {
                        dbc.ExecuteNonQuery();
                    }
                    finally
                    {
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "DELETE executed in {0} ms", sw.ElapsedMilliseconds);
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
                foreach (var itm in tableMap.Columns)
                {
                    var itmValue = itm.SourceProperty.GetValue(obj);
                    if (itm.IsPrimaryKey)
                        whereClause.And($"{itm.Name} = ?", itmValue);
                }

                var query = this.CreateSqlStatement<TModel>().DeleteFrom().Where(whereClause);
                lock (this.m_lockObject)
                {
                    var dbc = this.m_provider.CreateCommand(this, query);
                    try
                    {
                        dbc.ExecuteNonQuery();
                    }
                    finally
                    {
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "DELETE executed in {0} ms", sw.ElapsedMilliseconds);
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
                foreach (var itm in tableMap.Columns)
                {
                    var itmValue = itm.SourceProperty.GetValue(value);

                    if (itmValue == null ||
                        itmValue.Equals(default(Guid)) ||
                        itmValue.Equals(default(DateTime)) ||
                        itmValue.Equals(default(DateTimeOffset)) ||
                        itmValue.Equals(default(Decimal)))
                        itmValue = null;

                    else if (itm.IsHashed && itmValue != null)
                        using (var sha = System.Security.Cryptography.SHA256.Create())
                        {
                            if (itmValue is byte[])
                                itmValue = sha.ComputeHash(itmValue as byte[]);
                            else if (itmValue is String)
                                itmValue = BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(itmValue.ToString()))).Replace("-", "");
                        }

                    query.Append($"{itm.Name} = ? ", itmValue);
                    query.Append(",");
                    if (itm.IsPrimaryKey)
                        whereClause.And($"{itm.Name} = ?", itmValue);
                }
                query.RemoveLast();
                query.Where(whereClause);

                // Now update
                lock (this.m_lockObject)
                {
                    var dbc = this.m_provider.CreateCommand(this, query);
                    try
                    {
                        dbc.ExecuteNonQuery();
                    }
                    finally
                    {
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

                if (value is IAdoLoadedData)
                    (value as IAdoLoadedData).Context = this;

                return value;
#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "UPDATE executed in {0} ms", sw.ElapsedMilliseconds);
            }
#endif
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
                    var dbc = this.m_provider.CreateCommand(this, stmt);
                    try
                    {
                        dbc.ExecuteNonQuery();
                    }
                    finally
                    {
                        if (!this.IsPreparedCommand(dbc))
                            dbc.Dispose();
                    }
                }

#if DEBUG
            }
            finally
            {
                sw.Stop();
                this.m_tracer.TraceEvent(TraceEventType.Verbose, 0, "EXECUTE NON QUERY executed in {0} ms", sw.ElapsedMilliseconds);
            }
#endif
        }
    }
}
