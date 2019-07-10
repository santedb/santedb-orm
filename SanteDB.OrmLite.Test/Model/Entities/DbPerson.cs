﻿/*
 * Copyright 2015-2019 Mohawk College of Applied Arts and Technology
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
 * User: JustinFyfe
 * Date: 2019-1-22
 */
using SanteDB.Core.Model.Constants;
using SanteDB.OrmLite.Attributes;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents a person
    /// </summary>
    [Table("psn_tbl")]
	public class DbPerson : DbEntitySubTable
	{
        /// <summary>
        /// Parent key
        /// </summary>
        [JoinFilter(PropertyName = nameof(DbEntity.ClassConceptKey), Value = EntityClassKeyStrings.Person)]
        [JoinFilter(PropertyName = nameof(DbEntity.ClassConceptKey), Value = EntityClassKeyStrings.Patient)]
        public override Guid ParentKey
        {
            get
            {
                return base.ParentKey;
            }

            set
            {
                base.ParentKey = value;
            }
        }

        /// <summary>
        /// Gets or sets the date of birth.
        /// </summary>
        /// <value>The date of birth.</value>
        [Column("dob")]
		public DateTime? DateOfBirth {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the date of birth precision.
		/// </summary>
		/// <value>The date of birth precision.</value>
		[Column("dob_prec")]
		public string DateOfBirthPrecision {
			get;
			set;
		}


	}
}

