using SanteDB.Core.Diagnostics;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        /// Average time
        /// </summary>
        AverageTime = 3
    }

    /// <summary>
    /// ORM statistics performance probes
    /// </summary>
    internal class OrmClientProbe : ICompositeDiagnosticsProbe
    {

        // Lock object
        private static object m_lockObject = new object();

        // Registered probes
        private static readonly IDictionary<String, OrmClientProbe> m_registeredProbes = new Dictionary<String, OrmClientProbe>();

        // id
        private readonly Guid m_id = Guid.NewGuid();

        // Data provider
        private IDbProvider m_provider;

        // Component values
        private OrmPerformanceComponentProbe[] m_componentValues;

        /// <summary>
        /// Gets the values of this probe
        /// </summary>
        public IEnumerable<IDiagnosticsProbe> Value => this.m_componentValues.OfType<IDiagnosticsProbe>();

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
            /// Incrememnt the counter
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
            public override Int64 Value => Interlocked.Read(ref this.m_value);

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
            this.m_componentValues = new OrmPerformanceComponentProbe[4]
            {
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Readonly Connections", "Shows active readonly connections between this server and the database"),
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Read-Write Connections", "Shows active read/write connections between this server and the database"),
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Active Statements", "Shows the active statements between this server and the database")
#if DEBUG
                ,
                new OrmPerformanceComponentProbe(Guid.NewGuid(), "Average Result Time", "Shows the rolling average of result times in MS", "ms")
#endif
            };

            DiagnosticsProbeManager.Current.Add(this);
        }

        /// <summary>
        /// Create a probe
        /// </summary>
        public static OrmClientProbe CreateProbe(IDbProvider provider)
        {
            lock(m_lockObject)
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
        internal void Increment(OrmPerformanceMetric metric) => this.m_componentValues[(int)metric].Increment();

        /// <summary>
        /// Decrement the value
        /// </summary>
        internal void Decrement(OrmPerformanceMetric metric) => this.m_componentValues[(int)metric].Decrement();

        /// <summary>
        /// Average the time
        /// </summary>
        internal void AverageWith(OrmPerformanceMetric metric, long value) => this.m_componentValues[(int)metric].SetValue((this.m_componentValues[(int)metric].Value + value) / 2);

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
        public string Description => $"Shows metrics related to connections to {this.m_provider.GetDatabaseName()}";

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
