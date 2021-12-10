﻿using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.DataType
{
    /// <summary>
    /// Represents an assigning authority
    /// </summary>
    [Table("asgn_aut_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbAssigningAuthority : DbBaseData
    {

        /// <summary>
        /// Gets or sets the name of the aa
        /// </summary>
        [Column("aut_name")]
        public String Name { get; set; }

        /// <summary>
        /// Gets or sets the short HL7 code of the AA
        /// </summary>
        [Column("nsid")]
        public String DomainName { get; set; }

        /// <summary>
        /// Gets or sets the OID of the AA
        /// </summary>
        [Column("oid")]
        public String Oid { get; set; }

        /// <summary>
        /// Gets or sets the description of the AA
        /// </summary>
        [Column("descr")]
        public String Description { get; set; }

        /// <summary>
        /// Gets or sets the URL of AA
        /// </summary>
        [Column("url")]
        public String Url { get; set; }

        /// <summary>
        /// Assigning device identifier
        /// </summary>
        [Column("app_id"), ForeignKey(typeof(DbSecurityApplication), nameof(DbSecurityApplication.Key))]
        public Guid? AssigningApplicationKey { get; set; }

        /// <summary>
        /// Validation regular expression
        /// </summary>
        [Column("val_rgx")]
        public String ValidationRegex { get; set; }

        /// <summary>
        /// Gets or sets the key
        /// </summary>
        [Column("aut_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }

        /// <summary>
        /// True if the AA is unique
        /// </summary>
        [Column("is_unq")]
        public bool IsUnique { get; set; }
    }


    /// <summary>
    /// Identifier scope
    /// </summary>
    [ExcludeFromCodeCoverage]
    [Table("asgn_aut_scp_tbl")]
    public class DbAuthorityScope 
    {
        /// <summary>
        /// Gets or sets the scope of the auhority
        /// </summary>
        [Column("aut_id"), PrimaryKey, ForeignKey(typeof(DbAssigningAuthority), nameof(DbAssigningAuthority.Key))]
        public Guid AssigningAuthorityKey { get; set; }

        /// <summary>
        /// Gets or sets the scope of the auhority
        /// </summary>
        [Column("cd_id"), PrimaryKey, ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid ScopeConceptKey { get; set; }

    }
}
