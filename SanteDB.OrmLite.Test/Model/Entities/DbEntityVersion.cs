﻿using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents an entity in the database
    /// </summary>
    [Table("ent_vrsn_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbEntityVersion : DbVersionedData
    {
        /// <summary>
        /// Gets or sets the key
        /// </summary>
        [Column("ent_id"), ForeignKey(typeof(DbEntity), nameof(DbEntity.Key)), AlwaysJoin]
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the status concept identifier.
        /// </summary>
        /// <value>The status concept identifier.</value>
        [Column("sts_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
		public Guid StatusConceptKey {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the type concept identifier.
		/// </summary>
		/// <value>The type concept identifier.</value>
		[Column("typ_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
		public Guid? TypeConceptKey {
			get;
			set;
		}

        /// <summary>
        /// Gets or sets the version id
        /// </summary>
        [Column("ent_vrsn_id"), PrimaryKey, AutoGenerated]
        public override Guid VersionKey { get; set; }

        /// <summary>
        /// Creation act key
        /// </summary>
        [Column("crt_act_id")]
        public Guid? CreationActKey { get; set; }
    }
}

