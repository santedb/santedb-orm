using SanteDB.Core.Configuration;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace SanteDB.OrmLite.Configuration
{
    /// <summary>
    /// ORM Configuration Section
    /// </summary>
    [XmlType(nameof(OrmConfigurationSection), Namespace = "http://santedb.org/configuration")]
    public class OrmConfigurationSection : IConfigurationSection
    {
        /// <summary>
        /// ORM Configuration Section
        /// </summary>
        public OrmConfigurationSection()
        {
            this.Providers = new List<ProviderRegistrationConfiguration>();
        }

        /// <summary>
        /// Gets or sets the list of providers
        /// </summary>
        [XmlArray("providers"), XmlArrayItem("add")]
        public List<ProviderRegistrationConfiguration> Providers
        {
            get; set;
        }

        /// <summary>
        /// Get the specified provider
        /// </summary>
        public IDbProvider GetProvider(String invariant)
        {
            var provider = this.Providers.FirstOrDefault(o => o.Invariant.Equals(invariant, StringComparison.InvariantCultureIgnoreCase));
            if (provider == null)
                throw new KeyNotFoundException($"Provider {invariant} not registered");
            return Activator.CreateInstance(provider.Type) as IDbProvider;
        }
    }


    /// <summary>
    /// A class representing the registration of an invariant with a provider.
    /// </summary>
    [XmlType(nameof(ProviderRegistrationConfiguration), Namespace = "http://santedb.org/configuration")]
    public class ProviderRegistrationConfiguration : TypeReferenceConfiguration
    {

        /// <summary>
        /// Default ctor for serialization
        /// </summary>
        public ProviderRegistrationConfiguration()
        {

        }

        /// <summary>
        /// Create a new type with invariant
        /// </summary>
        public ProviderRegistrationConfiguration(string invariant, Type type) : base(type.AssemblyQualifiedName)
        {
            this.Invariant = invariant;
        }

        /// <summary>
        /// Gets or sets the invariant name
        /// </summary>
        [XmlAttribute("invariant")]
        public String Invariant { get; set; }
    }

}
