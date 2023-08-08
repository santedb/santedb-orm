/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-5-19
 */
using SanteDB.Core.Security.Configuration;
using SanteDB.OrmLite.Providers.Postgres;
using System.Collections.Generic;
using System.ComponentModel;
using System.Security.Cryptography.X509Certificates;
using System.Xml.Serialization;

namespace SanteDB.OrmLite.Configuration
{

    /// <summary>
    /// Identifies the method or mode of ALE 
    /// </summary>
    [XmlType(nameof(OrmAleMode), Namespace = "http://santedb.org/configuration")]
    public enum OrmAleMode
    {
        /// <summary>
        /// No ALE is enabled
        /// </summary>
        [XmlEnum("off")]
        Off = 0,
        /// <summary>
        /// The IV is calculated using a random value each encryption cycle
        /// </summary>
        [XmlEnum("random")]
        Random,
        /// <summary>
        /// The IV is calculated using a deterministic algorithm based on the input 
        /// </summary>
        [XmlEnum("deterministic")]
        Deterministic
    }

    /// <summary>
    /// Application level encryption configurations
    /// </summary>
    [XmlType(nameof(OrmAleConfiguration), Namespace = "http://santedb.org/configuration")]
    public class OrmAleConfiguration : IOrmEncryptionSettings
    {

        /// <summary>
        /// Gets or sets the ALE mode
        /// </summary>
        [XmlAttribute("aleMode")]
        [DisplayName("Mode")]
        [Description("Identifies the mode in which values are encrypted. Deterministic is less secure but provides faster queries, Randomized is more secure but may result in full-table scans for data query")]
        public OrmAleMode Mode { get; set; }

        /// <summary>
        /// Gets or sets the ALE certificate
        /// </summary>
        [DisplayName("Certificate")]
        [Description("The certificate to use for Application Level Encryption")]
        [XmlElement("certificate")]
        [TypeConverter(typeof(ExpandableObjectConverter))]
        public X509ConfigurationElement Certificate { get; set; }


        /// <summary>
        /// The fields to be enabled
        /// </summary>
        [XmlArray("fields"), XmlArrayItem("enable")]
        [Editor("System.Windows.Forms.Design.StringCollectionEditor, System.Design", "System.Drawing.Design.UITypeEditor, System.Drawing")]
        [DisplayName("Field List")]
        [Description("The fields on which ALE encryption should be enabled. Note that once enabled, encryption fields cannot used for queries other than exact matches")]
        public List<string> EnableFields { get; set; }


        /// <inheritdoc/>
        X509Certificate2 IOrmEncryptionSettings.Certificate => this.Certificate?.Certificate;

        /// <inheritdoc/>
        bool IOrmEncryptionSettings.ShouldEncrypt(string fieldName) => this.EnableFields.Contains(fieldName);
    }
}