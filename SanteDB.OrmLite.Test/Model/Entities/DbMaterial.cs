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
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;



namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents a material in the database
    /// </summary>
    [Table("mat_tbl")]
	public class DbMaterial : DbEntitySubTable
    {

        /// <summary>
        /// Parent key filter
        /// </summary>
        [JoinFilter(PropertyName = nameof(DbEntity.ClassConceptKey), Value = EntityClassKeyStrings.Material)]
        [JoinFilter(PropertyName = nameof(DbEntity.ClassConceptKey), Value = EntityClassKeyStrings.ManufacturedMaterial)]
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
        /// Gets or sets the quantity of an entity within its container.
        /// </summary>
        /// <value>The quantity.</value>
        [Column("qty")]
		public decimal Quantity {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the form concept.
		/// </summary>
		/// <value>The form concept.</value>
		[Column("frm_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
		public Guid FormConceptKey {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the quantity concept.
		/// </summary>
		/// <value>The quantity concept.</value>
		[Column("qty_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
		public Guid QuantityConceptKey {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the expiry date.
		/// </summary>
		/// <value>The expiry date.</value>
		[Column("exp_utc")]
		public DateTime ExpiryDate {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets a value indicating whether this instance is administrative.
		/// </summary>
		/// <value><c>true</c> if this instance is administrative; otherwise, <c>false</c>.</value>
		[Column("is_adm")]
		public bool IsAdministrative {
			get;
			set;
		}
	}

	/// <summary>
	/// Manufactured material.
	/// </summary>
	[Table("mmat_tbl")]
	public class DbManufacturedMaterial : DbEntitySubTable
	{

        /// <summary>
        /// Parent key filter
        /// </summary>
        [Column("ent_vrsn_id"), ForeignKey(typeof(DbMaterial), nameof(DbMaterial.ParentKey)), PrimaryKey, AlwaysJoin, JoinFilter(PropertyName = nameof(DbEntity.ClassConceptKey), Value = EntityClassKeyStrings.ManufacturedMaterial)]
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
        /// Gets or sets the lot number.
        /// </summary>
        /// <value>The lot number.</value>
        [Column("lot_no")]
		public String LotNumber {
			get;
			set;
		}
	}

}

