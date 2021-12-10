﻿using SanteDB.OrmLite.Attributes;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Security
{
    /// <summary>
    /// Represents a claim on a table
    /// </summary>
    [ExcludeFromCodeCoverage]
    [Table("sec_ses_clm_tbl")]
    public class DbSessionClaim : DbIdentified
    {

        /// <summary>
        /// Gets or sets the claim
        /// </summary>
        [Column("clm_id"), AutoGenerated ,PrimaryKey]
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the session key
        /// </summary>
        [Column("ses_id"), NotNull, ForeignKey(typeof(DbSession), nameof(DbSession.Key))]
        public Guid SessionKey { get; set; }

        /// <summary>
        /// Gets or sets the claim type
        /// </summary>
        [Column("clm_typ"), NotNull]
        public String ClaimType { get; set; }

        /// <summary>
        /// Gets or sets the claim value
        /// </summary>
        [Column("clm_val"), NotNull]
        public String ClaimValue { get; set; }


    }
}
