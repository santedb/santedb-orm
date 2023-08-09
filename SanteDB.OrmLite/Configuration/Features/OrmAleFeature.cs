using SanteDB.Core.Configuration;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Configuration.Features;
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
            foreach(var itm in this.m_configuration.Values)
            {
                var aleConfiguration = itm.Value as OrmAleConfiguration;
                switch(aleConfiguration.Mode) {
                    case OrmAleMode.Off:
                        yield return new DecryptAleDataTask(this, itm.Key, aleConfiguration);
                        break;
                    default:
                        yield return new EncryptAleDataTask(this, itm.Key, aleConfiguration);
                        break;
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
            bool databaseExists = true;
            foreach(var ormConfiguration in configuration.Sections.OfType<OrmConfigurationBase>())
            {
                try
                {
                    var ormSection = configuration.GetSection<OrmConfigurationSection>();
                    var connectionString = configuration.GetSection<DataConfigurationSection>()?.ConnectionString.Find(o=>o.Name.Equals(ormConfiguration.ReadWriteConnectionString, StringComparison.OrdinalIgnoreCase));
                    var providerType = ormSection?.Providers.Find(o => o.Invariant == connectionString.Provider).Type;
                    var provider = Activator.CreateInstance(providerType) as IEncryptedDbProvider;

                    if(provider == null)
                    {
                        continue;
                    }

                    provider.ConnectionString = connectionString.ToString();
                    if (!this.m_configuration.Options.ContainsKey(ormConfiguration.GetType().Name))
                    {
                        this.m_configuration.Options.Add(ormConfiguration.GetType().Name, () => ConfigurationOptionType.Object);
                        this.m_configuration.Values.Add(ormConfiguration.GetType().Name, ormConfiguration.AleConfiguration ?? new OrmAleConfiguration());
                    }

                    using (var conn = provider.GetWriteConnection())
                    {
                         conn.Open();
                         databaseExists &= conn.Any(new SqlStatement("SELECT 1 FROM patch_db_systbl WHERE patch_id = '20230802-01'"));
                    }
                }
                catch {
                    databaseExists = false;
                } // ignore exceptions here
            }

            return databaseExists ? configuration.Sections.OfType<OrmConfigurationBase>().Any(o => o.AleConfiguration != null) ? FeatureInstallState.Installed : FeatureInstallState.PartiallyInstalled : FeatureInstallState.CantInstall;

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
            public bool VerifyState(SanteDBConfiguration configuration)
            {
                return false;
            }
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
