﻿using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model.DataType
{
    /// <summary>
    /// Identifier type table.
    /// </summary>
    [Table("id_typ_tbl")]
	public class DbIdentifierType : DbBaseData
	{
        /// <summary>
        /// Gets or sets the id type
        /// </summary>
        [Column("id_typ_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the type concept identifier.
        /// </summary>
        /// <value>The type concept identifier.</value>
        [Column("typ_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
		public Guid TypeConceptKey {
			get;
			set;
		}

        /// <summary>
        /// Gets or sets the type concept identifier.
        /// </summary>
        /// <value>The type concept identifier.</value>
        [Column("ent_scp_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid? EntityScopeKey
        {
            get;
            set;
        }
    }
}

