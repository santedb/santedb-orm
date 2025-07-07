using DocumentFormat.OpenXml.Drawing.Diagrams;
using DocumentFormat.OpenXml.VariantTypes;
using SanteDB.Core;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Exceptions;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// ORM Provider manager
    /// </summary>
    public class OrmProviderManager : IDisposable
    {

        /// <summary>
        /// Current provider manager
        /// </summary>
        private static OrmProviderManager m_current;

        /// <summary>
        /// Lockbox
        /// </summary>
        private static object m_lock = new object();

        // Providers
        private readonly ConcurrentDictionary<String, IDbProvider> m_providers = new ConcurrentDictionary<String, IDbProvider>();

        /// <summary>
        /// Provider types
        /// </summary>
        private readonly Dictionary<string, Type> m_providerTypes;
        private readonly IConfigurationManager m_configurationManager;

        /// <summary>
        /// Create a new singleton
        /// </summary>
        private OrmProviderManager(IConfigurationManager configurationManager)
        {
            this.m_providerTypes = configurationManager.GetSection<OrmConfigurationSection>()?.Providers.ToDictionary(o=>o.Invariant, o=>o.Type);
            this.m_configurationManager = configurationManager;
        }

        /// <summary>
        /// Get the current ORM provider manager
        /// </summary>
        public static OrmProviderManager Current
        {
            get
            {
                if(m_current == null)
                {
                    lock(m_lock)
                    {
                        if(m_current == null)
                        {
                            m_current = new OrmProviderManager(ApplicationServiceContext.Current.GetService<IConfigurationManager>());
                            AppDomain.CurrentDomain.DomainUnload += (o, e) => m_current.Dispose();
                        }
                    }
                }
                return m_current;
            }
        }

        /// <summary>
        /// Dispose the connection and dependent providers
        /// </summary>
        public void Dispose()
        {
            foreach(var itm in this.m_providers.Values.OfType<IDisposable>())
            {
                itm.Dispose();
            }
        }

        /// <summary>
        /// Get the provider configured with the specified ORM configuration section
        /// </summary>
        public IDbProvider GetProvider(OrmConfigurationBase ormConfigurationSection)
        {
            if(this.m_providers.TryGetValue(ormConfigurationSection.ReadWriteConnectionString, out var retVal))
            {
                return retVal;
            }
            else if (this.m_providerTypes.TryGetValue(ormConfigurationSection.ProviderType, out var providerType))
            {
                retVal = (IDbProvider)providerType.CreateInjected();
                retVal.ReadonlyConnectionString = this.ResolveConnectionString(ormConfigurationSection.ReadonlyConnectionString);
                retVal.ConnectionString = this.ResolveConnectionString(ormConfigurationSection.ReadWriteConnectionString);
                retVal.TraceSql = ormConfigurationSection.TraceSql;
                if (ormConfigurationSection.AleConfiguration != null && retVal is IEncryptedDbProvider e)
                {
                    e.SetEncryptionSettings(ormConfigurationSection.AleConfiguration);
                }
                this.m_providers.TryAdd(ormConfigurationSection.ReadWriteConnectionString, retVal);
                return retVal;
            }
            else
            {
                throw new KeyNotFoundException($"Provider {ormConfigurationSection.ProviderType} not registered");
            }
        }

        /// <summary>
        /// Get a provider using the connection string provided
        /// </summary>
        /// <param name="connectionString">The connection string</param>
        public IDbProvider GetProvider(ConnectionString connectionString)
        {
            if (this.m_providers.TryGetValue(connectionString.Value, out var retVal))
            {
                return retVal;
            }
            else if (this.m_providerTypes.TryGetValue(connectionString.Provider, out var providerType))
            {
                retVal = (IDbProvider)providerType.CreateInjected();
                this.m_providers.TryAdd(connectionString.Value, retVal);
                return retVal;
            }
            else
            {
                throw new KeyNotFoundException($"Provider {connectionString.Provider} not registered");
            }
        }

        /// <summary>
        /// Resolve a connection string
        /// </summary>
        public string ResolveConnectionString(string connectionString)
        {
            var cstr = this.m_configurationManager.GetConnectionString(connectionString);
            if(cstr == null)
            {
                throw new KeyNotFoundException($"Connection string {connectionString} not found");
            }
            return cstr.ToString();
        }
    }
}
