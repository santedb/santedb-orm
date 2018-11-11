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
 * User: fyfej
 * Date: 2017-9-1
 */
using SanteDB.OrmLite.Attributes;
using System;
using System.Collections.Generic;
using System.Reflection;
using SanteDB.Core.Model;
using System.Linq;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Public key mapping
    /// </summary>
    public class PublicKeyMapping
    {

        // Column mapping
        private static Dictionary<PropertyInfo, PublicKeyMapping> s_publicKeyCache = new Dictionary<PropertyInfo, PublicKeyMapping>();

        /// <summary>
        /// Gets or sets the source property
        /// </summary>
        public PropertyInfo SourceProperty { get; set; }

        /// <summary>
        /// Gets the private key column mapping for this public key
        /// </summary>
        public ColumnMapping PrivateKey { get; private set; }

        /// <summary>
        /// Gets the target table
        /// </summary>
        public TableMapping TargetTable { get; private set; }

        /// <summary>
        /// Gets the target column
        /// </summary>
        public ColumnMapping TargetColumn { get; private set; }

        /// <summary>
        /// Public key mappings
        /// </summary>
        private PublicKeyMapping(PropertyInfo property, TableMapping table)
        {
            if (property.PropertyType.StripNullable() != typeof(Guid))
                throw new ArgumentException("Public key references must be of type Guid or Guid?");
            this.SourceProperty = property;
            var privateKeyName = property.GetCustomAttribute<PublicKeyRefAttribute>()?.LocalKey;
            this.PrivateKey = table.Columns.FirstOrDefault(o => o.SourceProperty.Name == privateKeyName);
            if (this.PrivateKey == null)
                throw new KeyNotFoundException($"Cannot find {privateKeyName}");
            this.TargetTable = this.PrivateKey.ForeignKey != null ? TableMapping.Get(this.PrivateKey.ForeignKey.Table) : null;
            this.TargetColumn = this.TargetTable?.GetColumn(this.PrivateKey.ForeignKey?.Column);
        }

        /// <summary>
        /// Get the property mapping
        /// </summary>
        public static PublicKeyMapping Get(PropertyInfo property, TableMapping ownerTable)
        {
            PublicKeyMapping retVal = null;
            if(!s_publicKeyCache.TryGetValue(property, out retVal))
                lock (s_publicKeyCache)
                {
                    retVal = new PublicKeyMapping(property, ownerTable);
                    if (!s_publicKeyCache.ContainsKey(property))
                        s_publicKeyCache.Add(property, retVal);
                }
            return retVal;
        }
    }
}