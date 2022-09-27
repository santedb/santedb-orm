/*
 * Copyright (C) 2021 - 2022, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2022-5-30
 */
using SanteDB.Core.Configuration;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
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
        /// Ado providers
        /// </summary>
        [XmlArray("dbProviderFactories"), XmlArrayItem("add")]
        public List<ProviderRegistrationConfiguration> AdoProvider { get; set; }

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
        public ProviderRegistrationConfiguration(string invariant, Type type) : base(type)
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
