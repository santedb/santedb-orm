﻿/*
 * Copyright (C) 2021 - 2025, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-6-21
 */
using SanteDB.OrmLite.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Table information tool
    /// </summary>
    public class TableMapping
    {
        // Hashmap
        private Dictionary<String, ColumnMapping> m_mappings = new Dictionary<string, ColumnMapping>();

        // Tabl mappings
        private static Dictionary<Type, TableMapping> m_tableMappings = new Dictionary<Type, TableMapping>();

        // Primary key cache
        private IEnumerable<ColumnMapping> m_primaryKey = null;

        /// <summary>
        /// ORM type model
        /// </summary>
        public Type OrmType { get; private set; }

        /// <summary>
        /// Table name
        /// </summary>
        public String TableName { get; private set; }

        /// <summary>
        /// True if the object has a table attribute
        /// </summary>
        public bool HasName { get; private set; }

        /// <summary>
        /// Column mappings
        /// </summary>
        public IEnumerable<ColumnMapping> Columns { get; private set; }

        /// <summary>
        /// Gets the primary key field
        /// </summary>
        public IEnumerable<ColumnMapping> PrimaryKey
        {
            get
            {
                if (this.m_primaryKey == null)
                {
                    this.m_primaryKey = this.Columns.Where(o => o.IsPrimaryKey);
                }

                return this.m_primaryKey;
            }
        }

        /// <summary>
        /// Private ctor for table mapping
        /// </summary>
        private TableMapping(Type t)
        {
            this.OrmType = t;
            this.TableName = t.GetCustomAttribute<TableAttribute>()?.Name ?? t.Name;
            this.HasName = this.TableName != t.Name;
            this.Columns = t.GetProperties().Where(o => o.GetCustomAttribute<ColumnAttribute>() != null).Select(o => ColumnMapping.Get(o, this)).ToList();
            foreach (var itm in this.Columns)
            {
                this.m_mappings.Add(itm.SourceProperty.Name, itm);
            }
        }

        /// <summary>
        /// Get table information by the name of the table
        /// </summary>
        public static TableMapping Get(String tableName)
        {
            return m_tableMappings.FirstOrDefault(o => o.Value.TableName == tableName).Value;
        }
        /// <summary>
        /// Get table information
        /// </summary>
        public static TableMapping Get(Type t)
        {
            TableMapping retVal = null;
            if (!m_tableMappings.TryGetValue(t, out retVal))
            {
                lock (m_tableMappings)
                {
                    retVal = new TableMapping(t);
                    if (!m_tableMappings.ContainsKey(t))
                    {
                        m_tableMappings.Add(t, retVal);
                    }
                }
            }

            return retVal;
        }

        /// <summary>
        /// Creates a table mapping that is redirected
        /// </summary>
        public static TableMapping Redirect(Type original, Type shadow)
        {
            var retVal = new TableMapping(original);
            var shadowMap = Get(shadow);
            var invalidMaps = retVal.m_mappings.Where(c => !shadowMap.Columns.Any(s => s.Name == c.Value.Name));
            foreach (var i in invalidMaps.ToArray())
            {
                retVal.m_mappings.Remove(i.Key);
            }

            retVal.TableName = shadowMap.TableName;
            return retVal;
        }

        /// <summary>
        /// Get column mapping
        /// </summary>
        public ColumnMapping GetColumn(PropertyInfo pi)
        {
            ColumnMapping map = null;
            this.m_mappings.TryGetValue(pi.Name, out map);
            return map;
        }

        /// <summary>
        /// Get column mapping
        /// </summary>
        public ColumnMapping GetColumn(MemberInfo mi)
        {
            ColumnMapping map = null;
            this.m_mappings.TryGetValue(mi.Name, out map);
            return map;
        }

        /// <summary>
        /// Get the column mapping for the named property
        /// </summary>
        public ColumnMapping GetColumn(string propertyName, bool scanHeirarchy = false)
        {
            ColumnMapping map = null;
            if (!this.m_mappings.TryGetValue(propertyName, out map) && scanHeirarchy &&
                this.OrmType.BaseType != typeof(Object))
            {
                var t = TableMapping.Get(this.OrmType.BaseType);
                return t.GetColumn(propertyName, scanHeirarchy);
            }
            return map;
        }

        /// <summary>
        /// Association with table type
        /// </summary>
        public TableMapping AssociationWith(Type otherTableType)
        {
            if (otherTableType == null)
            {
                return null;
            }
            else
            {
                return this.AssociationWith(TableMapping.Get(otherTableType));
            }
        }
        /// <summary>
        /// Gets the association table mapping
        /// </summary>
        public TableMapping AssociationWith(TableMapping subTableMap)
        {
            if (subTableMap == null)
            {
                return null;
            }
            var att = this.OrmType.GetCustomAttributes<AssociativeTableAttribute>().FirstOrDefault(o => o.TargetTable == subTableMap.OrmType);
            if (att == null)
            {
                return null;
            }
            else
            {
                return TableMapping.Get(att.AssociationTable);
            }
        }
    }
}