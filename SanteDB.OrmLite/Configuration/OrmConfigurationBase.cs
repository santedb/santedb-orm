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
using SanteDB.Core;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Providers;
using System;
using System.ComponentModel;
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
        [Category("Connection")]
        [DisplayName("Read/Write Connection String")]
        [Description("The primary connection to use for connecting to the SanteDB persistence layer")]
        [Editor("SanteDB.Configuration.Editors.ConnectionStringEditor, SanteDB.Configuration", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0")]
        public String ReadWriteConnectionString { get; set; }

        /// <summary>
        /// Readonly connection string
        /// </summary>
        [XmlAttribute("readOnlyConnectionString")]
        [Category("Connection")]
        [DisplayName("Read-Only Connection String")]
        [Description("The connection to use for readonly access (queries). This is used when you have a read replica (example: Streaming Replication on PostgreSQL) and want queries to be directed to the read replica")]
        [Editor("SanteDB.Configuration.Editors.ConnectionStringEditor, SanteDB.Configuration", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0")]
        public String ReadonlyConnectionString { get; set; }

        /// <summary>
        /// Trace SQL enabled
        /// </summary>
        [XmlAttribute("traceSql")]
        [Category("Diagnostics")]
        [DisplayName("Trace SQL")]
        [Description("When true, logs all generated SQL to the log file")]
        public bool TraceSql { get; set; }

        /// <summary>
        /// Provider type
        /// </summary>
        [XmlAttribute("providerType")]
        [Category("Connection")]
        [DisplayName("Data Provider")]
        [Description("The ORM provider to use for this connection")]
        [TypeConverter("SanteDB.Configuration.Converters.DataProviderConverter, SanteDB.Configuration")]
        [Editor("SanteDB.Configuration.Editors.DataProviderEditor, SanteDB.Configuration", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0")]
        public String ProviderType { get; set; }

        /// <summary>
        /// Get the provider
        /// </summary>
        [XmlIgnore, Browsable(false)]
        public IDbProvider Provider
        {
            get
            {
                if (this.m_dbProvider == null)
                {
                    this.m_dbProvider = ApplicationServiceContext.Current.GetService<IConfigurationManager>().GetSection<OrmConfigurationSection>().GetProvider(this.ProviderType);
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
        protected String ResolveConnectionString(String connectionStringName)
        {
            return ApplicationServiceContext.Current.GetService<IConfigurationManager>().GetConnectionString(connectionStringName)?.Value;
        }

    }
}
