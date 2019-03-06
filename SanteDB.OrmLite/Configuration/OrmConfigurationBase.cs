﻿/*
 * Copyright 2015-2019 Mohawk College of Applied Arts and Technology
 * Copyright 2019-2019 SanteSuite Contributors (See NOTICE)
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
 * User: justi
 * Date: 2019-1-12
 */
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.ComponentModel;
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
        [Category("Connection")]
        [Description("The primary connection to use for connecting to the SanteDB persistence layer")]
        [Editor("SanteDB.Configuration.Editors.ConnectionStringEditor, SanteDB.Configuration, Version=1.10.0.0", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0")]
        public String ReadWriteConnectionString { get; set; }

        /// <summary>
        /// Readonly connection string
        /// </summary>
        [XmlAttribute("readOnlyConnectionString")]
        [Category("Connection")]
        [Description("The connection to use for readonly access (queries). This is used when you have a read replica (example: Streaming Replication on PostgreSQL) and want queries to be directed to the read replica")]
        [Editor("SanteDB.Configuration.Editors.ConnectionStringEditor, SanteDB.Configuration, Version=1.10.0.0", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0")]
        public String ReadonlyConnectionString { get; set; }

        /// <summary>
        /// Trace SQL enabled
        /// </summary>
        [XmlAttribute("traceSql")]
        [Category("Diagnostics")]
        [Description("When true, logs all generated SQL to the log file")]
        public bool TraceSql { get; set; }

        /// <summary>
        /// Provider type
        /// </summary>
        [XmlAttribute("providerType")]
        [Category("Connection")]
        [Description("The ORM provider to use for this connection")]
        [TypeConverter("SanteDB.Configuration.Converters.DataProviderConverter, SanteDB.Configuration, Version=1.10.0.0")]
        [Editor("SanteDB.Configuration.Editors.DataProviderEditor, SanteDB.Configuration, Version=1.10.0.0", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0")]
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
