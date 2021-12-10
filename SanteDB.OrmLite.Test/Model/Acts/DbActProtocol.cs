﻿using SanteDB.OrmLite.Attributes;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Acts
{
    /// <summary>
    /// Represents a link between act and protocol
    /// </summary>
    [ExcludeFromCodeCoverage]
    [Table("act_proto_assoc_tbl")]
    public class DbActProtocol : DbAssociation
    {

        /// <summary>
        /// Gets or sets the key
        /// </summary>
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the protocol key
        /// </summary>
        [Column("proto_id"), ForeignKey(typeof(DbProtocol), nameof(DbProtocol.Key)), PrimaryKey]
        public Guid ProtocolKey { get; set; }

        /// <summary>
        /// Source key
        /// </summary>
        [Column("act_id"), ForeignKey(typeof(DbAct), nameof(DbAct.Key)), PrimaryKey]
        public override Guid SourceKey { get; set; }

        /// <summary>
        /// Gets or sets the state
        /// </summary>
        [Column("state_dat")]
        public byte[] State { get; set; }

        /// <summary>
        /// Sequence
        /// </summary>
        [Column("seq")]
        public int Sequence { get; set; }

        /// <summary>
        /// Gets or sets the complete flag
        /// </summary>
        [Column("is_compl")]
        public bool IsComplete { get; set; }

    }
}
