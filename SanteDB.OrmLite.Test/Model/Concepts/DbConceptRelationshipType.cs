﻿using SanteDB.OrmLite.Attributes;
using System;
using System.Diagnostics.CodeAnalysis;


namespace SanteDB.Persistence.Data.ADO.Data.Model.Concepts
{
	/// <summary>
	/// Concept relationship type.
	/// </summary>
	[ExcludeFromCodeCoverage]
	[Table("cd_rel_typ_cdtbl")]
	public class DbConceptRelationshipType: DbNonVersionedBaseData
	{

		/// <summary>
		/// Gets or sets the name.
		/// </summary>
		/// <value>The name.</value>
		[Column("rel_name")]
		public String Name {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the mnemonic.
		/// </summary>
		/// <value>The mnemonic.</value>
		[Column("mnemonic")]
		public String Mnemonic {
			get;
			set;
		}

        /// <summary>
        /// Gets or sets the key
        /// </summary>
        [Column("rel_typ_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }
    }
}

