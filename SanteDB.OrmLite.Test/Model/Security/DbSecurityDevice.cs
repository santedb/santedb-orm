﻿using SanteDB.OrmLite.Attributes;
using System;
using System.Diagnostics.CodeAnalysis;


namespace SanteDB.Persistence.Data.ADO.Data.Model.Security
{
    /// <summary>
    /// Represents a security device. This table should only have one row (the current device)
    /// </summary>
    [Table("sec_dev_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbSecurityDevice : DbBaseData
	{
		
		/// <summary>
		/// Gets or sets the public identifier.
		/// </summary>
		/// <value>The public identifier.</value>
		[Column("dev_pub_id")]
		public String PublicId {
			get;
			set;
		}

        /// <summary>
        /// Device secret
        /// </summary>
        [Column("dev_scrt"), Secret]
        public String DeviceSecret { get; set; }

        /// <summary>
        /// Replaces the specified device identifier
        /// </summary>
        [Column("rplc_dev_id")]
        public Guid? ReplacesDeviceKey { get; set; }

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

        /// <summary>
        /// Gets or sets the key
        /// </summary>
        [Column("dev_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }
    }
}

