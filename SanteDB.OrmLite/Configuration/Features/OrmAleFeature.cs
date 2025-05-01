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
 * Date: 2024-6-21
 */
using SanteDB.Core.Configuration;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Configuration.Features;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Linq;

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
        public FeatureFlags Flags => FeatureFlags.None;

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
                yield return new EncryptAleDataTask(this, itm.Key, aleConfiguration);
            }
        }

        /// <inheritdoc/>
        public IEnumerable<IConfigurationTask> CreateUninstallTasks()
        {
            return this.m_configuration.Values.Select(o =>
            {
                var conf = o.Value as OrmAleConfiguration;
                conf.AleEnabled = false;
                conf.Certificate = null;
                return new EncryptAleDataTask(this, o.Key, conf);
            });
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
                            if (!this.m_configuration.Options.ContainsKey(ormConfiguration.ReadWriteConnectionString))
                            {
                                this.m_configuration.Options.Add(ormConfiguration.ReadWriteConnectionString, () => ConfigurationOptionType.Object);
                                this.m_configuration.Values.Add(ormConfiguration.ReadWriteConnectionString, ormConfiguration.AleConfiguration ?? new OrmAleConfiguration());
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
            private readonly Tracer m_tracer = Tracer.GetTracer(typeof(OrmAleFeature));

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
            public string Name => $"Re-Encrypt {this.m_ormSection}";

            /// <inheritdoc/>
            public event EventHandler<ProgressChangedEventArgs> ProgressChanged;

            /// <inheritdoc/>
            public bool Execute(SanteDBConfiguration configuration)
            {
                // We want to execute the orm recrypt function
                var ormConfigurations = configuration.Sections.OfType<OrmConfigurationBase>().Where(o => o.ReadWriteConnectionString == this.m_ormSection);

                var currentlyEnabled = ormConfigurations.All(o => o.AleConfiguration?.AleEnabled != true);
                var ormSection = configuration.GetSection<OrmConfigurationSection>();
                var connectionString = configuration.GetSection<DataConfigurationSection>()?.ConnectionString.Find(o => o.Name.Equals(ormConfigurations.First().ReadWriteConnectionString, StringComparison.OrdinalIgnoreCase));
                var providerType = ormSection?.Providers.Find(o => o.Invariant == connectionString.Provider).Type;
                var provider = Activator.CreateInstance(providerType) as IEncryptedDbProvider;

                if (provider == null)
                {
                    this.m_tracer.TraceWarning("Cannot install ALE - provider not found");
                    return false;
                }

                provider.ConnectionString = connectionString.ToString();

                // Decrypt - 
                this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs(0.0f, "Rotating ALE Keys"));
                if (provider is IReportProgressChanged irpc && this.ProgressChanged != null)
                {
                    irpc.ProgressChanged += this.ProgressChanged;
                }

                using (AuthenticationContext.EnterSystemContext())
                {
                    var config = new OrmAleConfiguration()
                    {
                        AleEnabled = this.m_aleConfiguration.AleEnabled,
                        Certificate = this.m_aleConfiguration.Certificate != null ? new Core.Security.Configuration.X509ConfigurationElement(this.m_aleConfiguration.Certificate) : null,
                        SaltSeed = this.m_aleConfiguration.SaltSeed,
                        EnableFields = new List<OrmFieldConfiguration>(this.m_aleConfiguration.EnableFields)
                    };

                    if (ormConfigurations.First().AleConfiguration != null) // decrypt and recrypt
                    {
                        provider.SetEncryptionSettings(ormConfigurations.First().AleConfiguration);
                        provider.GetEncryptionProvider();
                        provider.MigrateEncryption(config);
                    }
                    else
                    {
                        provider.SetEncryptionSettings(config);
                        provider.GetEncryptionProvider();
                    }
                    ormConfigurations.ToList().ForEach(o => o.AleConfiguration = config);
                }

                return true;
            }

            /// <inheritdoc/>
            public bool Rollback(SanteDBConfiguration configuration)
            {
                throw new NotImplementedException();
            }

            /// <inheritdoc/>
            public bool VerifyState(SanteDBConfiguration configuration) => this.m_aleConfiguration.AleEnabled &&
                this.m_aleConfiguration.Certificate != null &&
                this.m_aleConfiguration.Certificate.Certificate.HasPrivateKey &&
                this.m_aleConfiguration.SaltSeedXml != null &&
                this.m_aleConfiguration.EnableFields?.Count() > 0 ||
                !this.m_aleConfiguration.AleEnabled;
        }
    }
}
