﻿using SanteDB.OrmLite.Attributes;
using System;
using System.Diagnostics.CodeAnalysis;


namespace SanteDB.Persistence.Data.ADO.Data.Model.Security
{
    /// <summary>
    /// Security applicationDb Should only be one entry here as well
    /// </summary>
    [Table("sec_app_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbSecurityApplication : DbBaseData
	{

        /// <summary>
        /// Gets or sets the application id
        /// </summary>
        [Column("app_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the public identifier.
        /// </summary>
        /// <value>The public identifier.</value>
        [Column("app_pub_id")]
		public String PublicId {
			get;
			set;
		}

        /// <summary>
        /// Application authentication secret
        /// </summary>
        [Column("app_scrt"), Secret]
        public String Secret { get; set; }

        /// <summary>
        /// Replaces application identifier
        /// </summary>
        [Column("rplc_app_id")]
        public Guid? ReplacesApplicationKey { get; set; }

        /// <summary>
        /// Gets or sets the lockout
        /// </summary>
        [Column("locked")]
        public DateTimeOffset? Lockout { get; set; }

        /// <summary>
        /// Gets or sets the lockout
        /// </summary>
        [Column("fail_auth")]
        public int? InvalidAuthAttempts { get; set; }

        /// <summary>
        /// Gets the last authenticated time
        /// </summary>
        [Column("last_auth_utc")]
        public DateTimeOffset? LastAuthentication { get; set; }
    }
}

