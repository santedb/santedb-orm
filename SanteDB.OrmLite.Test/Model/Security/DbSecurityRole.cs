﻿using SanteDB.OrmLite.Attributes;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Security
{
    /// <summary>
    /// Represents a security role
    /// </summary>
    [Table("sec_rol_tbl")]
	public class DbSecurityRole : DbNonVersionedBaseData
	{

        /// <summary>
        /// Gets or sets the role id
        /// </summary>
        [Column("rol_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        /// <value>The name.</value>
        [Column("rol_name")]
		public String Name {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the description.
		/// </summary>
		/// <value>The description.</value>
		[Column("descr")]
		public String Description {
			get;
			set;
		}

	}
}

