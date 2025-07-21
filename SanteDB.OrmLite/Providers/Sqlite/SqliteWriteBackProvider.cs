using DocumentFormat.OpenXml.InkML;
using DocumentFormat.OpenXml.Office2016.Drawing.ChartDrawing;
using DocumentFormat.OpenXml.Wordprocessing;
using SanteDB.Core;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.i18n;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Attributes;
using SanteDB.OrmLite.Diagnostics;
using SharpCompress;
using System;
using System.CodeDom;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// An implementation of a SQLite data provider that creates write-back connections
    /// </summary>
    public class SqliteWriteBackProvider : SqliteProvider, IDisposable, IReportProgressChanged, IDbWriteBackProvider
    {

        /// <summary>
        /// Schema information
        /// </summary>
        private class DbSchemaObject
        {

            /// <summary>
            /// Gets or sets the name of the object
            /// </summary>
            [Column("name")]
            public string Name { get; set; }

            /// <summary>
            /// Gets or sets the SQL to create the object
            /// </summary>
            [Column("sql")]
            public string Sql { get; set; }

            /// <summary>
            /// Gets or sets the type of object
            /// </summary>
            [Column("type")]
            public string Type { get; set; }

            /// <summary>
            /// Column names
            /// </summary>
            public String[] Columns { get; set; }

            /// <summary>
            /// Gets or sets the primary keys
            /// </summary>
            public String[] PrimaryKeys { get; set; }
        }

        /// <summary>
        /// Last writeback flush
        /// </summary>
        private long m_lastWritebackFlush = 0;

        /// <summary>
        /// Max ticks between a flush
        /// </summary>
        private const long MAX_TICKS_BETWEEN_FLUSH = TimeSpan.TicksPerSecond * 30;

        /// <summary>
        /// Maximum flush requests
        /// </summary>
        private const int MAX_FLUSH_REQUESTS = 30;

        /// <summary>
        /// Tracer
        /// </summary>
        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(SqliteWriteBackProvider));

        /// <summary>
        /// Disk write-back connections
        /// </summary>
        private static readonly ConcurrentDictionary<String, DbSchemaObject[]> m_initializedWritebackCaches = new ConcurrentDictionary<string, DbSchemaObject[]>();

        /// <summary>
        /// Lock=object
        /// </summary>
        private static readonly object m_lockObject = new object();

        /// <summary>
        /// Reset event
        /// </summary>
        private readonly ManualResetEventSlim m_pingBackgroundWriteThread = new ManualResetEventSlim(false);

        /// <summary>
        /// Ping the disposal thread
        /// </summary>
        private readonly ManualResetEventSlim m_pingDisposalThread = new ManualResetEventSlim(false);

        /// <summary>
        /// Writeback thread
        /// </summary>
        private readonly Thread m_writebackCacheThread;

        /// <summary>
        /// The number of flush requests waiting
        /// </summary>
        private long m_writebackCacheFlushRequests = 0;

        /// <summary>
        /// Disposed
        /// </summary>
        private bool m_runMonitor = true;

        /// <summary>
        /// Writeback provider
        /// </summary>
        public SqliteWriteBackProvider()
        {
            this.m_tracer.TraceInfo("Creating new instance of SqliteWritebackProvider");
            this.m_writebackCacheThread = new Thread(this.MonitorWritebackFlush)
            {
                IsBackground = true,
                Name = $"SQLite+WB Flush {this.GetHashCode()}"
            };
        }

        /// <inheritdoc/>
        public override string GetDatabaseName() => $"wb_{base.GetDatabaseName()}";

        /// <inheritdoc/>
        public override DataContext GetWriteConnection()
        {
            if (InitializeWritebackCache(base.GetDatabaseName()))
            {
                var connection = this.GetProviderFactory().CreateConnection();
                connection.ConnectionString = this.GetCacheConnectionString(false);
                var retVal = new DataContext(this, connection);
                retVal.Disposed += (o, e) => this.RequestWritebackFlush();
                return retVal;
            }
            else
            {
                return base.GetWriteConnection();
            }
        }

        /// <summary>
        /// Initialize the cached database
        /// </summary>
        private bool InitializeWritebackCache(string databaseName)
        {
            lock (m_lockObject)
            {
                // Prevent multiple threads from initializing the writeback cache 
                if (!m_initializedWritebackCaches.TryGetValue(databaseName, out var schemaObjects))
                {
                    try
                    {
                        this.m_tracer.TraceInfo("Initializing writeback cache for {0} (current writeback caches: {1})", databaseName, String.Join(";", m_initializedWritebackCaches.Keys));


                        using (var cacheConnection = this.GetProviderFactory().CreateConnection())
                        {
                            this.m_lockoutEvent.Wait(); // Allow the underlying Sqlite provider to prevent us from opening the disk connection
                            try
                            {

                                using (var context = new ReaderWriterLockingDataContext(this, cacheConnection))
                                {
                                    cacheConnection.ConnectionString = this.GetCacheConnectionString(false);
                                    context.Open(initializeExtensions: false);
                                    this.m_lockoutEvent.Reset();

                                    // Attach the file db
                                    var connectionString = SqliteProvider.CorrectConnectionString(new ConnectionString(this.Invariant, base.ReadonlyConnectionString));
                                    var basePassword = connectionString.GetComponent("Password");
                                    var fileLocation = connectionString.GetComponent("Data Source");
                                    if (!String.IsNullOrEmpty(basePassword))
                                    {
                                        this.m_tracer.TraceVerbose("Attempting to attach database '{0}' using '{1}'", fileLocation, basePassword);
                                        cacheConnection.Execute($"ATTACH '{fileLocation}' AS fs KEY '{basePassword}'");
                                    }
                                    else
                                    {
                                        cacheConnection.Execute($"ATTACH '{fileLocation}' AS fs");
                                    }

                                    // Extract from file and load the memory cache
                                    this.m_tracer.TraceVerbose("Initializing tables...");
                                    schemaObjects = context.ExecQuery<DbSchemaObject>(new SqlStatement("SELECT DISTINCT name, sql, type FROM fs.sqlite_master WHERE name NOT LIKE 'sqlite%'")).ToArray();
                                    schemaObjects.Where(o => o.Type == "table").ForEach(obj => cacheConnection.Execute(obj.Sql)); // Create the tables    

                                    // Initialize the schema
                                    this.m_tracer.TraceVerbose("Fetching table schemas...");
                                    schemaObjects.Where(o => o.Type == "table").ForEach(table =>
                                    {
                                        this.m_tracer.TraceVerbose("Fetching schema {0}...", table.Name);
                                        var metadata = context.Query<ExpandoObject>(new SqlStatement($"PRAGMA table_info('{table.Name}')")).OfType<IDictionary<string, Object>>().ToArray();
                                        table.Columns = metadata.Select(o => o["name"].ToString()).ToArray();
                                        table.PrimaryKeys = metadata.Where(o => 1l.Equals(o["pk"])).Select(o => o["name"].ToString()).ToArray();
                                    });

                                    this.m_tracer.TraceVerbose("Seeding cache data...");

                                    foreach (var itm in schemaObjects.Where(t => t.Type == "table"))
                                    {
                                        context.ExecuteNonQuery($"INSERT INTO {itm.Name} ({String.Join(",", itm.Columns)}) SELECT {String.Join(",", itm.Columns)} FROM fs.{itm.Name}");
                                    }

                                    this.m_tracer.TraceVerbose("Initializing indexes and triggers...");
                                    schemaObjects.Where(o => o.Type != "table").ForEach(cmd => cacheConnection.Execute(cmd.Sql)); // Create the views, indexes, and triggers    

                                    context.ExecuteNonQuery("DETACH DATABASE fs");
                                    Interlocked.Exchange(ref this.m_lastWritebackFlush, DateTimeOffset.Now.Ticks); 
                                }
                            }
                            finally
                            {
                                this.m_lockoutEvent.Set();
                            }
                            m_initializedWritebackCaches.TryAdd(databaseName, schemaObjects);
                            if (!this.m_writebackCacheThread.IsAlive)
                            {
                                this.m_writebackCacheThread.Start(); // Start the monitoring thread
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        this.m_tracer.TraceError("Error initializing write-back cache for {0} - revert to file system - {1}", databaseName, ex);
                        schemaObjects = null;
                        m_initializedWritebackCaches.TryAdd(databaseName, null);
                    }
                }
                else
                {
                    int schemaObjectCount = 0;
                    using (var cacheConnection = this.GetProviderFactory().CreateConnection())
                    {
                        cacheConnection.ConnectionString = this.GetCacheConnectionString(true);
                        cacheConnection.Open();
                        schemaObjectCount = cacheConnection.ExecuteScalar<int>("SELECT COUNT(name) FROM sqlite_master WHERE name NOT LIKE 'sqlite%'");
                    }
                    if (schemaObjectCount == 0) // Our cache is gone 😔
                    {
                        if (m_initializedWritebackCaches.TryRemove(databaseName, out _))
                        {
                            return this.InitializeWritebackCache(databaseName);
                        }
                    }
                }
                return schemaObjects != null;
            }
        }

        /// <summary>
        /// Monitor the writeback flush process
        /// </summary>
        private void MonitorWritebackFlush(object state)
        {
            var ocp = this.MonitorProbe as OrmClientProbe;
            while (this.m_runMonitor)
            {
                try
                {
                    ocp.Set(OrmPerformanceMetric.WritebackFlushes, Interlocked.Read(ref this.m_writebackCacheFlushRequests));
                    ocp.Set(OrmPerformanceMetric.WriteBackAge, (DateTimeOffset.Now.Ticks - Interlocked.Read(ref this.m_lastWritebackFlush)) / TimeSpan.TicksPerSecond);
                    this.m_pingBackgroundWriteThread.Wait(3000);
                    this.FlushWriteBackToDisk(!this.m_runMonitor);
                }
                catch (ThreadAbortException)
                {
                    try
                    {
                        this.FlushWriteBackToDisk(true);
                    }
                    catch (Exception e)
                    {
                        this.m_tracer.TraceError("Error writing out the writeback cache: {0}", e.ToHumanReadableString());
                    }
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Error writing out the writeback cache: {0}", e.ToHumanReadableString());
                }
                finally
                {
                    this.m_pingBackgroundWriteThread.Reset();
                }
            }
            m_pingDisposalThread.Set();
        }

        /// <summary>
        /// Flush the write-back cache
        /// </summary>
        private void FlushWriteBackToDisk(bool force)
        {

            var waitingFlushRequests = Interlocked.Read(ref this.m_writebackCacheFlushRequests);
            var ticksSinceLastWrite = DateTimeOffset.Now.Ticks - Interlocked.Read(ref this.m_lastWritebackFlush);
            if (m_initializedWritebackCaches.TryGetValue(base.GetDatabaseName(), out var dbSchemaObjects) && dbSchemaObjects != null && (force || waitingFlushRequests > MAX_FLUSH_REQUESTS || waitingFlushRequests > 0 && ticksSinceLastWrite > MAX_TICKS_BETWEEN_FLUSH)) // There were changes
            {
                this.m_tracer.TraceInfo("Flushing Writeback to Disk for {0}", this.GetDatabaseName());
                Interlocked.Exchange(ref this.m_writebackCacheFlushRequests, 0);

                this.m_lockoutEvent.Wait(); // Allow the underlying Sqlite provider to prevent us from opening the disk connection
                try
                {
                    using (var flushConn = base.GetWriteConnectionInternal(false))
                    {
                        // Prevent other connections from opening on the backend 
                        this.m_lockoutEvent.Reset();
                        flushConn.Open(initializeExtensions: false);
                        flushConn.Connection.Execute($"ATTACH 'file:{this.GetDatabaseName()}?mode=memory&cache=shared' AS ms");

                        // We want to create any changed tables or
                        // indexes
                        var i = 0;
                        foreach (var tbl in dbSchemaObjects.Where(o => o.Type == "table"))
                        {
                            this.FireProgressChanged(new ProgressChangedEventArgs($"WriteBack:{this.GetDatabaseName()}", (float)i++ / (float)dbSchemaObjects.Length, UserMessages.FLUSHING_CACHE));
                            this.m_tracer.TraceVerbose("Flushing {0}", tbl.Name);

                            flushConn.ExecuteNonQuery($"DELETE FROM {tbl.Name};");

                            // Get the column names
                            flushConn.ExecuteNonQuery($"INSERT INTO {tbl.Name} ({String.Join(",", tbl.Columns)}) SELECT {String.Join(",", tbl.Columns)} FROM ms.{tbl.Name}");
                        }

                        flushConn.Connection.Execute("DETACH DATABASE ms");
                    }

                    this.m_tracer.TraceInfo("Writeback has been flushed to {0}", this.GetDatabaseName());
                    Interlocked.Exchange(ref this.m_lastWritebackFlush, DateTimeOffset.Now.Ticks);
                }
                finally
                {
                    this.m_lockoutEvent.Set(); // allow other threads to access the disk file
                }
            }
        }

        /// <summary>
        /// Flush the contents of the memory cache into the disk
        /// </summary>
        private void RequestWritebackFlush()
        {
            this.m_tracer.TraceVerbose("Requesting flush of writeback {0} - {1} waiting requests", this.GetDatabaseName(), Interlocked.Increment(ref this.m_writebackCacheFlushRequests));
            this.m_pingBackgroundWriteThread.Set();
        }

        /// <inheritdoc/>
        public override DataContext GetReadonlyConnection()
        {
            if (InitializeWritebackCache(base.GetDatabaseName()))
            {
                var connection = this.GetProviderFactory().CreateConnection();
                connection.ConnectionString = this.GetCacheConnectionString(true);
                return new DataContext(this, connection);
            }
            else
            {
                return base.GetReadonlyConnection();
            }
        }

        /// <summary>
        /// Create cache connection string to the writeback cache
        /// </summary>
        private string GetCacheConnectionString(bool isReadonly) => $"Data Source=file:{this.GetDatabaseName()}?mode=memory&cache=shared;Foreign Keys=false; Mode={(isReadonly ? "ReadOnly" : "ReadWriteCreate")}";

        /// <summary>
        /// Dispose the threads
        /// </summary>
        public override void Dispose()
        {
            this.m_runMonitor = false;
            this.m_pingBackgroundWriteThread.Set();
            this.m_tracer.TraceInfo("Waiting for flush of write-back cache");
            this.m_pingDisposalThread.Wait();
            base.Dispose();
        }

        /// <inheritdoc/>
        public DataContext GetPersistentConnection() => base.GetWriteConnectionInternal();

        /// <summary>
        /// Optimize the database on disk not in memory
        /// </summary>
        public override void Optimize()
        {
            this.FlushWriteBackToDisk(true);
            base.Optimize();
            using (var writer = base.GetWriteConnectionInternal())
            {
                this.m_tracer.TraceInfo("Optimizing {0}...", this.GetDatabaseName());
                writer.Open(initializeExtensions: false);
                writer.ExecuteNonQuery(this.StatementFactory.CreateSqlKeyword(SqlKeyword.Vacuum));
                writer.ExecuteNonQuery(this.StatementFactory.CreateSqlKeyword(SqlKeyword.Reindex));
                writer.ExecuteNonQuery(this.StatementFactory.CreateSqlKeyword(SqlKeyword.Analyze));
                writer.ExecuteNonQuery("PRAGMA wal_checkpoint(truncate)");
            }
        }

        /// <inheritdoc/>
        public void FlushWriteBackCache() => this.FlushWriteBackToDisk(true);

        /// <inheritdoc/>
        public override void InitializeConnection(IDbConnection conn)
        {
            conn.Execute("PRAGMA synchronous=OFF");
            conn.ExecuteScalar<object>("PRAGMA pragma_automatic_index=true");
            conn.ExecuteScalar<Object>("PRAGMA locking_mode=normal");

            if (ApplicationServiceContext.Current.HostType == SanteDBHostType.Client) // clients have their check constraints disabled
            {
                conn.Execute("PRAGMA ignore_check_constraints=ON");
                conn.Execute("PRAGMA foreign_keys=FALSE");
                conn.Execute("PRAGMA journal_mode=MEMORY");
                conn.Execute("PRAGMA temp_store=MEMORY");
            }
        }
    }
}
