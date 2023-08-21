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
using Newtonsoft.Json;
using SanteDB.Core.Security.Configuration;
using SanteDB.OrmLite.Providers.Postgres;
using SharpCompress;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
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
        // Field settings
        private Dictionary<string, OrmAleMode> m_fieldSettings = null;

        // Mode
        private bool m_enable;

        /// <summary>
        /// Create new configuration
        /// </summary>
        public OrmAleConfiguration()
        {
            this.SaltSeed = new byte[16];
            RandomNumberGenerator.Create().GetBytes(this.SaltSeed);
        }

        /// <summary>
        /// Gets or sets the ALE mode
        /// </summary>
        [XmlAttribute("enabled")]
        [DisplayName("Enable ALE")]
        [Description("Identifies the mode in which values are encrypted. Deterministic is less secure but provides faster queries, Randomized is more secure but may result in full-table scans for data query")]
        public bool AleEnabled
        {
            get => this.m_enable;
            set
            {
                this.m_enable = value;

                if (!value)
                {
                    this.Certificate = null;
                }
                else
                {
                    this.Certificate = new X509ConfigurationElement();
                }
            }
        }

        /// <summary>
        /// Gets or sets the ALE certificate
        /// </summary>
        [DisplayName("Certificate")]
        [Description("The certificate to use for Application Level Encryption")]
        [XmlElement("certificate")]
        [TypeConverter(typeof(ExpandableObjectConverter))]
        public X509ConfigurationElement Certificate { get; set; }

        /// <summary>
        /// Salted SEED XML
        /// </summary>
        [DisplayName("Salting Seed")]
        [Description("A random (but consistent string) set by the administrator to salt the IV values in Deterministic method of encrypting (HEX ENCODED)")]
        [XmlElement("ivSeed")]
        public string SaltSeedXml
        {
            get => this.SaltSeed.HexEncode();
            set => this.SaltSeed = value.HexDecode();
        }

        /// <summary>
        /// Salted seed
        /// </summary>
        [XmlIgnore, JsonIgnore]
        [Browsable(false)]
        public byte[] SaltSeed { get; set; }

        /// <summary>
        /// The fields to be enabled
        /// </summary>
        [XmlElement("field")]
        [Editor("System.Windows.Forms.Design.StringCollectionEditor, System.Design", "System.Drawing.Design.UITypeEditor, System.Drawing")]
        [DisplayName("Field List")]
        [Description("The fields on which ALE encryption should be enabled. Note that once enabled, encryption fields cannot used for queries other than exact matches")]
        [Editor("SanteDB.Configuration.Editors.AleFieldSelectorEditor, SanteDB.Configuration", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0")]
        public List<OrmFieldConfiguration> EnableFields { get; set; }


        /// <inheritdoc/>
        X509Certificate2 IOrmEncryptionSettings.Certificate => this.Certificate?.Certificate;

        /// <summary>
        /// Disable all fields
        /// </summary>
        internal void DisableAll()
        {
            this.m_fieldSettings = this.EnableFields.Select(o => new OrmFieldConfiguration() { Name = o.Name, Mode = OrmAleMode.Off }).ToDictionary(o => o.Name, o => OrmAleMode.Off);
        }

        /// <inheritdoc/>
        bool IOrmEncryptionSettings.ShouldEncrypt(string fieldName, out OrmAleMode configuredMode)
        {
            configuredMode = OrmAleMode.Off;
            if (this.m_fieldSettings == null)
            {
                this.m_fieldSettings = this.EnableFields.ToDictionaryIgnoringDuplicates(o => o.Name, o => o.Mode);
            }
            return !string.IsNullOrEmpty(fieldName) && this.m_fieldSettings.TryGetValue(fieldName, out configuredMode) && this.m_enable;
        }
    }

    /// <summary>
    /// ORM field configuration
    /// </summary>
    [XmlType(nameof(OrmFieldConfiguration), Namespace = "http://santedb.org/configuration")]
    public class OrmFieldConfiguration
    {

        /// <summary>
        /// Gets or sets the name
        /// </summary>
        [XmlText]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the mode 
        /// </summary>
        [XmlAttribute("mode")]
        public OrmAleMode Mode { get; set; }

        /// <inheritdoc/>
        public override string ToString() => $"{this.Name}({this.Mode})";

    }
}