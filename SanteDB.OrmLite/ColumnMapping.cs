﻿/*
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
using System.Linq;
using System.Reflection;
using SanteDB.Core.Model;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Column mapping
    /// </summary>
    public class ColumnMapping
    {

        // Specified property
        private PropertyInfo m_specifiedProperty = null;

        // Private/public mappings
        private PublicKeyMapping m_publicKeyMapping = null;
        private bool m_publicKeyMappingScanned = false;

        // Column mapping
        private static Dictionary<PropertyInfo, ColumnMapping> s_columnCache = new Dictionary<PropertyInfo, ColumnMapping>();

        /// <summary>
        /// Gets the source property
        /// </summary>
        public PropertyInfo SourceProperty { get; private set; }

        /// <summary>
        /// Gets the name of the column
        /// </summary>
        public String Name { get; private set; }

        /// <summary>
        /// True if column is primary key
        /// </summary>
        public bool IsPrimaryKey { get; private set; }

        /// <summary>
        /// True if column is set by database
        /// </summary>
        public bool IsAutoGenerated { get; private set; }

        /// <summary>
        /// Gets the foreign key 
        /// </summary>
        public ForeignKeyAttribute ForeignKey { get; private set; }

        /// <summary>
        /// Join filters
        /// </summary>
        public List<JoinFilterAttribute> JoinFilters { get; private set; }

        /// <summary>
        /// The table mapping which this column belongs to
        /// </summary>
        public TableMapping Table { get; private set; }
        /// <summary>
        /// True if always join condition
        /// </summary>
        public bool IsAlwaysJoin { get; private set; }

        /// <summary>
        /// Identifies the column must always have a value even if 0
        /// </summary>
        public bool IsNonNull { get; private set; }

        /// <summary>
        /// True if the column is secret
        /// </summary>
        public bool IsSecret { get; private set; }

        /// <summary>
        /// True if the column is a public UUID
        /// </summary>
        public bool IsPublicKey { get; private set; }

        /// <summary>
        /// Gets the public key reference for this column if it is private key
        /// </summary>
        public PublicKeyMapping PublicKeyRef
        {
            get
            {
                if(!this.m_publicKeyMappingScanned)
                {
                    this.m_publicKeyMapping = this.Table.PublicKeyRefs.FirstOrDefault(o => o.PrivateKey.Name == this.Name);
                    this.m_publicKeyMappingScanned = true;
                }
                return this.m_publicKeyMapping;
            }
        }

        /// <summary>
        /// Create a column mapping
        /// </summary>
        private ColumnMapping(PropertyInfo pi, TableMapping table)
        {
            var att = pi.GetCustomAttribute<ColumnAttribute>();
            this.Name = att?.Name ?? pi.Name;
            this.IsPrimaryKey = pi.GetCustomAttribute<PrimaryKeyAttribute>() != null;
            this.IsSecret = pi.GetCustomAttribute<SecretAttribute>() != null;
            this.IsAutoGenerated = pi.GetCustomAttribute<AutoGeneratedAttribute>() != null;
            this.SourceProperty = pi;
            this.ForeignKey = pi.GetCustomAttribute<ForeignKeyAttribute>();
            this.IsNonNull = pi.GetCustomAttribute<NotNullAttribute>() != null;
            this.Table = table;
            this.IsPublicKey = pi.GetCustomAttribute<PublicKeyAttribute>() != null;
            if (this.IsPublicKey && pi.PropertyType.StripNullable() != typeof(Guid))
                throw new InvalidOperationException("Only UUIDs can be public keys");

            this.IsAlwaysJoin = pi.GetCustomAttribute<AlwaysJoinAttribute>() != null;
            this.JoinFilters = pi.GetCustomAttributes<JoinFilterAttribute>().ToList();
        }

        /// <summary>
        /// Get property mapping
        /// </summary>
        public static ColumnMapping Get(PropertyInfo pi, TableMapping ownerTable)
        {

            ColumnMapping retVal = null;
            if(!s_columnCache.TryGetValue(pi, out retVal)) 
                lock(s_columnCache)
                {
                    retVal = new ColumnMapping(pi, ownerTable);
                    if (!s_columnCache.ContainsKey(pi))
                        s_columnCache.Add(pi, retVal);
                }
            return retVal;
        }

        /// <summary>
        /// If the item has a Specified property then return its value
        /// </summary>
        public bool SourceSpecified(Object value)
        {
            if (m_specifiedProperty == null)
                this.m_specifiedProperty = this.SourceProperty.DeclaringType.GetRuntimeProperty($"{this.SourceProperty.Name}Specified");
            return (bool)(this.m_specifiedProperty?.GetValue(value) ?? false);
        }
    }
}