﻿using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;



namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents a telecommunications address
    /// </summary>
    [Table("ent_tel_tbl")]
	public class DbTelecomAddress : DbEntityVersionedAssociation
	{
        /// <summary>
        /// Gets or sets the primary key
        /// </summary>
        [Column("tel_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the telecom use.
        /// </summary>
        /// <value>The telecom use.</value>
        [Column("use_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
		public Guid TelecomUseKey {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the value.
		/// </summary>
		/// <value>The value.</value>
		[Column("tel_val")]
		public String Value {
			get;
			set;
		}

	}
}

