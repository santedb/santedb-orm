/*
 * Copyright (C) 2021 - 2024, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
using System;
using System.Linq;

namespace SanteDB.OrmLite.Attributes
{
    /// <summary>
    /// Represents a foreign key
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, Inherited = true)]
    public class ForeignKeyAttribute : Attribute
    {
        /// <summary>
        /// Creates a new foreign key attribute
        /// </summary>
        public ForeignKeyAttribute(Type table, String column)
        {
            this.Table = table;
            this.Column = column;
        }

        /// <summary>
        /// Gets or sets the table to which the key applies
        /// </summary>
        public Type Table { get; set; }

        /// <summary>
        /// Gets or sets the column to which the key applies
        /// </summary>
        public String Column { get; set; }

        /// <summary>
        /// Returns TRUE if this foreign key can be fulfilled by a join on <paramref name="otherTable"/>
        /// </summary>
        /// <param name="otherTable">The other table</param>
        internal bool CanQueryFrom(Type otherTable)
        {
            return this.Table == otherTable ||
                TableMapping.Get(otherTable).Columns.Any(c => c.ForeignKey != null && c.ForeignKey.Table == this.Table);
        }
    }
}
