/*
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
 * User: justin
 * Date: 2018-11-26
 */
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
    /// Represents a base ORM configuration object
    /// </summary>
    public abstract class OrmConfigurationBase
    {
        // DB Provider
        private IDbProvider m_dbProvider;

        /// <summary>
        /// Read/write connection string
        /// </summary>
        [XmlAttribute("readWriteConnectionString")]
        public String ReadWriteConnectionString { get; set; }

        /// <summary>
        /// Readonly connection string
        /// </summary>
        [XmlAttribute("readOnlyConnectionString")]
        public String ReadonlyConnectionString { get; set; }

        /// <summary>
        /// Trace SQL enabled
        /// </summary>
        [XmlAttribute("traceSql")]
        public bool TraceSql { get; set; }

        /// <summary>
        /// Provider type
        /// </summary>
        [XmlAttribute("providerType")]
        public String ProviderType { get; set; }

        /// <summary>
        /// Get the provider
        /// </summary>
        [XmlIgnore]
        public IDbProvider Provider
        {
            get
            {
                if (this.m_dbProvider == null)
                {
                    var dbt = Type.GetType(this.ProviderType);
                    if (dbt == null) throw new InvalidOperationException($"Type {this.ProviderType} could not be found");
                    this.m_dbProvider = Activator.CreateInstance(dbt) as IDbProvider;
                    if (this.m_dbProvider == null) throw new InvalidOperationException($"Type {this.ProviderType} does not implement IDbProvider");
                    this.m_dbProvider.ReadonlyConnectionString = this.ResolveConnectionString(this.ReadonlyConnectionString);
                    this.m_dbProvider.ConnectionString = this.ResolveConnectionString(this.ReadWriteConnectionString);
                    this.m_dbProvider.TraceSql = this.TraceSql;

                }
                return this.m_dbProvider;
            }

        }

        /// <summary>
        /// Resolves connection string
        /// </summary>
        protected abstract String ResolveConnectionString(String connectionStringName);

    }
}
