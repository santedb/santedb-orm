﻿/*
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
using SanteDB.OrmLite.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Column mapping
    /// </summary>
    public class ColumnMapping
    {
        /// <summary>
        /// Column mapper
        /// </summary>
        public class ColumnComparer : IEqualityComparer<ColumnMapping>
        {
            /// <summary>
            /// x == y
            /// </summary>
            public bool Equals(ColumnMapping x, ColumnMapping y) => x.Name.Equals(y.Name);

            /// <summary>
            /// Get hash code
            /// </summary>
            public int GetHashCode(ColumnMapping obj) => obj.Name.GetHashCode();
        }

        // Specified property
        private PropertyInfo m_specifiedProperty = null;

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
        /// True if the column is unique
        /// </summary>
        public bool IsUnique { get; private set; }

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
        /// Gets the default value
        /// </summary>
        public object DefaultValue { get; private set; }

        /// <summary>
        /// Column mapping ctor
        /// </summary>
        private ColumnMapping()
        { }

        /// <summary>
        /// Create a column mapping
        /// </summary>
        private ColumnMapping(PropertyInfo pi, TableMapping table)
        {
            var att = pi.GetCustomAttribute<ColumnAttribute>();
            this.Name = att?.Name ?? pi.Name;
            this.IsPrimaryKey = pi.HasCustomAttribute<PrimaryKeyAttribute>();
            this.IsSecret = pi.HasCustomAttribute<SecretAttribute>();
            this.IsAutoGenerated = pi.HasCustomAttribute<AutoGeneratedAttribute>();
            this.SourceProperty = pi;
            this.ForeignKey = pi.GetCustomAttribute<ForeignKeyAttribute>();
            this.IsUnique = pi.HasCustomAttribute<UniqueAttribute>();
            this.IsNonNull = pi.HasCustomAttribute<NotNullAttribute>();
            this.Table = table;
            this.IsAlwaysJoin = pi.HasCustomAttribute<AlwaysJoinAttribute>();
            this.JoinFilters = pi.GetCustomAttributes<JoinFilterAttribute>().ToList();
            this.DefaultValue = pi.GetCustomAttribute<DefaultValueAttribute>()?.DefaultValue;
            if (this.DefaultValue is String str && Guid.TryParse(str, out Guid defaultGuid))
            {
                this.DefaultValue = defaultGuid;
            }
        }

        /// <summary>
        /// Get property mapping
        /// </summary>
        public static ColumnMapping Get(String columnName)
        {
            return new ColumnMapping() { Name = columnName };
        }

        /// <summary>
        /// Get property mapping
        /// </summary>
        public static ColumnMapping Get(PropertyInfo pi, TableMapping ownerTable)
        {
            ColumnMapping retVal = null;
            if (!s_columnCache.TryGetValue(pi, out retVal))
            {
                lock (s_columnCache)
                {
                    retVal = new ColumnMapping(pi, ownerTable);
                    if (!s_columnCache.ContainsKey(pi))
                    {
                        s_columnCache.Add(pi, retVal);
                    }
                }
            }

            return retVal;
        }

        /// <summary>
        /// If the item has a Specified property then return its value
        /// </summary>
        public bool SourceSpecified(Object value)
        {
            if (m_specifiedProperty == null)
            {
                this.m_specifiedProperty = this.SourceProperty.DeclaringType.GetRuntimeProperty($"{this.SourceProperty.Name}Specified");
            }

            return (bool)(this.m_specifiedProperty?.GetValue(value) ?? false);
        }

        /// <summary>
        /// A column mapping representing 1 (for using in SELECT 1 FROM)
        /// </summary>
        public static readonly ColumnMapping One = new ColumnMapping()
        {
            Name = "1",
            SourceProperty = null
        };

        /// <summary>
        /// A column mapping representing 1 (for using in SELECT 1 FROM)
        /// </summary>
        public static readonly ColumnMapping Star = new ColumnMapping()
        {
            Name = "*",
            SourceProperty = null
        };

        /// <summary>
        /// Represent as a string
        /// </summary>
        public override string ToString() => $"{this.Table.TableName}.{this.Name}";

    }
}