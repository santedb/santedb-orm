using DocumentFormat.OpenXml.Drawing.Diagrams;
using SanteDB.Core.Configuration;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Configuration.Features;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Providers;
using SanteDB.OrmLite.Providers.Postgres;
using SharpCompress;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SanteDB.OrmLite.Configuration.Features
{
    /// <summary>
    /// A configuration feature for the setup and encryption of columns
    /// </summary>
    public class OrmAleFeature : IFeature
    {
        // Tracer
        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(OrmAleFeature));

        // Configuration
        private GenericFeatureConfiguration m_configuration;

        /// <inheritdoc/>
        public object Configuration
        {
            get => this.m_configuration;
            set => this.m_configuration = value as GenericFeatureConfiguration;
        }

        /// <inheritdoc/>
        public Type ConfigurationType => typeof(GenericFeatureConfiguration);

        /// <inheritdoc/>
        public string Description => "Settings related to Application Level Encryption";

        /// <inheritdoc/>
        public FeatureFlags Flags => FeatureFlags.None | FeatureFlags.AlwaysConfigure;

        /// <inheritdoc/>
        public string Group => FeatureGroup.Security;

        /// <inheritdoc/>
        public string Name => "Application Layer Encryption";

        /// <inheritdoc/>
        public IEnumerable<IConfigurationTask> CreateInstallTasks()
        {
            foreach (var itm in this.m_configuration.Values)
            {
                var aleConfiguration = itm.Value as OrmAleConfiguration;
                if (aleConfiguration.AleEnabled)
                {
                    yield return new EncryptAleDataTask(this, itm.Key, aleConfiguration);
                }
                else
                {
                    yield return new DecryptAleDataTask(this, itm.Key, aleConfiguration);
                }
            }
        }

        /// <inheritdoc/>
        public IEnumerable<IConfigurationTask> CreateUninstallTasks()
        {
            return this.m_configuration.Values.Select(o => new DecryptAleDataTask(this, o.Key, o.Value as OrmAleConfiguration));
        }

        /// <inheritdoc/>
        public FeatureInstallState QueryState(SanteDBConfiguration configuration)
        {
            this.m_configuration = this.m_configuration ?? new GenericFeatureConfiguration();

            // Create configuration for each of the connection strings
            foreach (var ormConfiguration in configuration.Sections.OfType<OrmConfigurationBase>())
            {
                try
                {
                    var ormSection = configuration.GetSection<OrmConfigurationSection>();
                    var connectionString = configuration.GetSection<DataConfigurationSection>()?.ConnectionString.Find(o => o.Name.Equals(ormConfiguration.ReadWriteConnectionString, StringComparison.OrdinalIgnoreCase));
                    var providerType = ormSection?.Providers.Find(o => o.Invariant == connectionString.Provider).Type;
                    var provider = Activator.CreateInstance(providerType) as IEncryptedDbProvider;

                    if (provider == null)
                    {
                        continue;
                    }

                    provider.ConnectionString = connectionString.ToString();
                    using (var conn = provider.GetWriteConnection())
                    {
                        conn.Open();
                        try
                        {
                            conn.Any(new SqlStatement("SELECT * FROM ale_systbl"));
                            if (!this.m_configuration.Options.ContainsKey(connectionString.Name))
                            {
                                this.m_configuration.Options.Add(connectionString.Name, () => ConfigurationOptionType.Object);
                                this.m_configuration.Values.Add(connectionString.Name, ormConfiguration.AleConfiguration ?? new OrmAleConfiguration());
                            }
                        }
                        catch
                        {
                            this.m_tracer.TraceWarning("Cannot enable ALE on {0} - SMK patching has not been installed", ormConfiguration.GetType().Name);
                        }
                    }
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Cannot determine ALE availability on {0} ({1})", ormConfiguration.GetType().Name, e.Message);
                } // ignore exceptions here
            }

            return configuration.Sections.OfType<OrmConfigurationBase>().Any(o => o.AleConfiguration != null) ? FeatureInstallState.Installed : FeatureInstallState.NotInstalled;

        }

        /// <summary>
        /// Encrypt data task
        /// </summary>
        private class EncryptAleDataTask : IConfigurationTask
        {
            private readonly string m_ormSection;
            private readonly OrmAleConfiguration m_aleConfiguration;

            public EncryptAleDataTask(OrmAleFeature ownerFeature, string ormSectionName, OrmAleConfiguration configuration)
            {
                this.Feature = ownerFeature;
                this.m_ormSection = ormSectionName;
                this.m_aleConfiguration = configuration;
            }

            /// <inheritdoc/>
            public string Description => $"Encrypt the selected columns in {this.m_ormSection} using application level encryption";

            /// <inheritdoc/>
            public IFeature Feature { get; }

            /// <inheritdoc/>
            public string Name => $"Encrypt {this.m_ormSection}";

            /// <inheritdoc/>
            public event EventHandler<ProgressChangedEventArgs> ProgressChanged;

            /// <inheritdoc/>
            public bool Execute(SanteDBConfiguration configuration)
            {
                throw new NotImplementedException();
            }

            /// <inheritdoc/>
            public bool Rollback(SanteDBConfiguration configuration)
            {
                throw new NotImplementedException();
            }

            /// <inheritdoc/>
            public bool VerifyState(SanteDBConfiguration configuration) => this.m_aleConfiguration.Certificate != null &&
                this.m_aleConfiguration.Certificate.Certificate.HasPrivateKey &&
                this.m_aleConfiguration.SaltSeedXml != null &&
                this.m_aleConfiguration.EnableFields?.Count() > 0;
        }

        /// <summary>
        /// Decrypt data task
        /// </summary>
        private class DecryptAleDataTask : IConfigurationTask
        {
            private readonly string m_ormSection;
            private readonly OrmAleConfiguration m_aleConfiguration;

            public DecryptAleDataTask(OrmAleFeature ownerFeature, string ormSectionName, OrmAleConfiguration configuration)
            {
                this.Feature = ownerFeature;
                this.m_ormSection = ormSectionName;
                this.m_aleConfiguration = configuration;
            }

            /// <inheritdoc/>
            public string Description => $"Decrypt the selected columns in {this.m_ormSection} using application level encryption";

            /// <inheritdoc/>
            public IFeature Feature { get; }

            /// <inheritdoc/>
            public string Name => $"Decrypt {this.m_ormSection}";

            /// <inheritdoc/>
            public event EventHandler<ProgressChangedEventArgs> ProgressChanged;

            /// <inheritdoc/>
            public bool Execute(SanteDBConfiguration configuration)
            {
                throw new NotImplementedException();
            }

            /// <inheritdoc/>
            public bool Rollback(SanteDBConfiguration configuration)
            {
                throw new NotImplementedException();
            }

            /// <inheritdoc/>
            public bool VerifyState(SanteDBConfiguration configuration)
            {
                return false;
            }
        }

    }
}
