/*
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
using SanteDB.Core.Diagnostics;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace SanteDB.OrmLite.Diagnostics
{
    /// <summary>
    /// ORM performance metrics
    /// </summary>
    internal enum OrmPerformanceMetric
    {
        /// <summary>
        /// Readonly connections
        /// </summary>
        ReadonlyConnections = 0,
        /// <summary>
        /// Read/write connections
        /// </summary>
        ReadWriteConnections = 1,
        /// <summary>
        /// Active statements
        /// </summary>
        ActiveStatements = 2,
        /// <summary>
        /// Connections which are waiting for a lock
        /// </summary>
        AwaitingLock = 3,
        /// <summary>
        /// Average time
        /// </summary>
        AverageTime = 4,

    }

    /// <summary>
    /// ORM statistics performance probes
    /// </summary>
    internal class OrmClientProbe : ICompositeDiagnosticsProbe, IDisposable
    {

        // Lock object
        private static object m_lockObject = new object();

        // Registered probes
        private static readonly IDictionary<String, OrmClientProbe> m_registeredProbes = new Dictionary<String, OrmClientProbe>();

        // Update
        private ConcurrentQueue<KeyValuePair<OrmPerformanceMetric, long>> m_queueInstructions = new ConcurrentQueue<KeyValuePair<OrmPerformanceMetric, long>>();

        // MRE for sending updates
        private ManualResetEventSlim m_resetEvent = new ManualResetEventSlim(false);

        // Thread for posting updates
        private readonly Thread m_postingThread;

        // id
        private readonly Guid m_id = Guid.NewGuid();

        // Disposed signal for the background processing thread
        private bool m_disposed = false;

        // Data provider
        private IDbProvider m_provider;

        // Component values
        private OrmPerformanceComponentProbe[] m_componentValues;

        /// <summary>
        /// Gets the values of this probe
        /// </summary>
        public IEnumerable<IDiagnosticsProbe> Value => this.m_componentValues.ToArray();

        /// <summary>
        /// ORM performance probe
        /// </summary>
        private class OrmPerformanceComponentProbe : DiagnosticsProbeBase<Int64>
        {

            private Int64 m_value;

            /// <summary>
            /// ORM performance probe
            /// </summary>
            public OrmPerformanceComponentProbe(Guid id, String name, String description, string unit = null) : base(name, description)
            {
                this.Uuid = id;
                this.Unit = unit;
            }

            /// <summary>
            /// Set the value
            /// </summary>
            public void SetValue(Int64 value) => Interlocked.Exchange(ref this.m_value, value);

            /// <summary>
            /// Increment the counter
            /// </summary>
            public void Increment()
            {
                Interlocked.Increment(ref this.m_value);
            }

            /// <summary>
            /// decrememnt the counter
            /// </summary>
            public void Decrement()
            {
                Interlocked.Decrement(ref this.m_value);
            }

            /// <summary>
            /// Gets the value of the probe
            /// </summary>
            public override Int64 Value => this.m_value;

            /// <summary>
            /// Gets the UUID for this object
            /// </summary>
            public override Guid Uuid { get; }

            /// <summary>
            /// Gets the units
            /// </summary>
            public override string Unit { get; }
        }

        /// <summary>
        /// The provider 
        /// </summary>
        private OrmClientProbe(IDbProvider provider)
        {
            this.m_provider = provider;
            this.m_postingThread = new Thread(this.UpdateOrmMetricsWorker)
            {
                Priority = ThreadPriority.Lowest,
                IsBackground = true,
                Name = $"{provider.GetDatabaseName()} monitor"
            };
            this.m_postingThread.Start();
#if DEBUG
            this.m_componentValues = new OrmPerformanceComponentProbe[]
            {
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Read-Only Connections", "Shows active read-only connections between this server and the read-only database pool"),
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Read-Write Connections", "Shows active read/write connections between this server and the database pool"),
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Active Statements", "Shows the active statements between this server and the database pool"),
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Waiting for Lock", "Shows the number of connections which are waiting for a read or write lock to continue opening"),
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Average Execution Time", "Shows the rolling average of DbCommand result times in MS", "ms")
        };
#else
            this.m_componentValues = new OrmPerformanceComponentProbe[]
{
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Read-Only Connections", "Shows active read-only connections between this server and the read-only database pool"),
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Read-Write Connections", "Shows active read/write connections between this server and the database pool"),
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Active Statements", "Shows the active statements between this server and the database pool"),
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Waiting for Lock", "Shows the number of connections which are waiting for a read or write lock to continue opening")

};

#endif

            DiagnosticsProbeManager.Current?.Add(this);
        }

        /// <summary>
        /// ORM update metrics
        /// </summary>
        private void UpdateOrmMetricsWorker(object obj)
        {

            while (!this.m_disposed)
            {
                try
                {
                    this.m_resetEvent.Wait(1000);
                    while (this.m_queueInstructions.TryDequeue(out var instruction))
                    {
                        switch (instruction.Key)
                        {
                            case OrmPerformanceMetric.AverageTime:
                                this.m_componentValues[(int)instruction.Key].SetValue(instruction.Value);
                                break;
                            default:
                                if (instruction.Value > 0)
                                {
                                    this.m_componentValues[(int)instruction.Key].Increment();
                                }
                                else if (instruction.Value < 0)
                                {
                                    this.m_componentValues[(int)instruction.Key].Decrement();
                                }

                                break;
                        }
                    }
                    this.m_resetEvent.Reset();
                }
                catch
                {
                }
            }
        }

        /// <summary>
        /// Create a probe
        /// </summary>
        public static OrmClientProbe CreateProbe(IDbProvider provider)
        {
            lock (m_lockObject)
            {
                if (!m_registeredProbes.TryGetValue(provider.GetDatabaseName(), out var retVal))
                {
                    retVal = new OrmClientProbe(provider);
                    m_registeredProbes.Add(provider.GetDatabaseName(), retVal);
                }
                return retVal;
            }
        }

        /// <summary>
        /// Increment the value
        /// </summary>
        internal void Increment(OrmPerformanceMetric metric)
        {
            this.m_queueInstructions.Enqueue(new KeyValuePair<OrmPerformanceMetric, long>(metric, 1));
            this.m_resetEvent.Set();
        }

        /// <summary>
        /// Decrement the value
        /// </summary>
        internal void Decrement(OrmPerformanceMetric metric)
        {
            this.m_queueInstructions.Enqueue(new KeyValuePair<OrmPerformanceMetric, long>(metric, -1));
            this.m_resetEvent.Set();
        }

        /// <summary>
        /// Average the time
        /// </summary>
        internal void AverageWith(OrmPerformanceMetric metric, long value)
        {
            this.m_queueInstructions.Enqueue(new KeyValuePair<OrmPerformanceMetric, long>(metric, value));
            this.m_resetEvent.Set();
        }

        /// <summary>
        /// Dispose the object
        /// </summary>
        public void Dispose()
        {
            if (this.m_disposed)
            {
                throw new ObjectDisposedException(nameof(OrmClientProbe));
            }

            this.m_disposed = true;
            this.m_resetEvent.Set();
            this.m_resetEvent.Dispose();
        }

        /// <summary>
        /// Gets the UUID of this probe
        /// </summary>
        public Guid Uuid => this.m_id;

        /// <summary>
        /// Gets the name of this performance probe
        /// </summary>
        public string Name => $"{this.m_provider.GetDatabaseName()} Client Metrics";

        /// <summary>
        /// Get the description of this field
        /// </summary>
        public string Description => $"Metrics for client database connections to {this.m_provider.GetDatabaseName()}";

        /// <summary>
        /// Get the type of metric
        /// </summary>
        public Type Type => typeof(Array);

        /// <summary>
        /// Gets the value
        /// </summary>
        object IDiagnosticsProbe.Value => this.m_provider;

        /// <summary>
        /// Get the units
        /// </summary>
        public string Unit => null;
    }

}
