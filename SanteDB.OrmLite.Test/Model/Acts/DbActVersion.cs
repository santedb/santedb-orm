﻿using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Acts
{
    /// <summary>
    /// Represents a table which can store act data
    /// </summary>
    [Table("act_vrsn_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbActVersion : DbVersionedData
    {
        /// <summary>
        /// True if negated
        /// </summary>
        [Column("neg_ind")]
        public bool IsNegated { get; set; }

        /// <summary>
        /// Identifies the time that the act occurred
        /// </summary>
        [Column("act_utc")]
        public DateTime? ActTime { get; set; }

        /// <summary>
        /// Identifies the start time of the act
        /// </summary>
        [Column("act_start_utc")]
        public DateTime? StartTime { get; set; }

        /// <summary>
        /// Identifies the stop time of the act
        /// </summary>
        [Column("act_stop_utc")]
        public DateTime? StopTime { get; set; }

        /// <summary>
        /// Gets or sets the reason concept
        /// </summary>
        [Column("rsn_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid ReasonConceptKey { get; set; }

        /// <summary>
        /// Gets or sets the status concept
        /// </summary>
        [Column("sts_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid StatusConceptKey { get; set; }

        /// <summary>
        /// Gets or sets the type concept
        /// </summary>
        [Column("typ_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid TypeConceptKey { get; set; }

        /// <summary>
        /// Version identifier
        /// </summary>
        [Column("act_vrsn_id"), PrimaryKey, AutoGenerated]
        public override Guid VersionKey { get; set; }
        
        /// <summary>
        /// Gets or sets the act identifier
        /// </summary>
        [Column("act_id"), ForeignKey(typeof(DbAct), nameof(DbAct.Key)), AlwaysJoin]
        public override Guid Key { get; set; }
    }
}
