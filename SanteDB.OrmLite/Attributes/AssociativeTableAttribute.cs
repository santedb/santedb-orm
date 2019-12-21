/*
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
 * User: Justin Fyfe
 * Date: 2019-8-8
 */
using System;

namespace SanteDB.OrmLite.Attributes
{
    /// <summary>
    /// Indicates that another table is associated with the current table through a third
    /// table
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class AssociativeTableAttribute : Attribute
    {

        /// <summary>
        /// Creates an associative table attribute
        /// </summary>
        public AssociativeTableAttribute(Type targetTable, Type associativeTable)
        {
            this.TargetTable = targetTable;
            this.AssociationTable = associativeTable;
        }

        /// <summary>
        /// Gets or sets the target table
        /// </summary>
        public Type TargetTable { get; set; }

        /// <summary>
        /// Gets or sets the table whicih associates the target table with the current table
        /// </summary>
        public Type AssociationTable { get; set; }

    }
}
