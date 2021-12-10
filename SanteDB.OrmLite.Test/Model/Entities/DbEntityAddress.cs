﻿using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents one or more entity addresses linked to an Entity
    /// </summary>
    [Table("ent_addr_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbEntityAddress : DbEntityVersionedAssociation
    {
        /// <summary>
        /// Gets or sets the key
        /// </summary>
        [Column("addr_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the use concept identifier.
        /// </summary>
        /// <value>The use concept identifier.</value>
        [Column("use_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid UseConceptKey
        {
            get;
            set;
        }

        ///// <summary>
        ///// Gets or sets the address sequence id
        ///// </summary>
        //[Column("addr_seq_id"), AutoGenerated]
        //public decimal? AddressSequenceId
        //{
        //    get;
        //    set;
        //}
    }

    /// <summary>
    /// Represents an identified address component
    /// </summary>
    [Table("ent_addr_cmp_tbl")]
    public class DbEntityAddressComponent : DbGenericNameComponent
    {
        /// <summary>
        /// Gets or sets the address identifier.
        /// </summary>
        /// <value>The address identifier.</value>
        [Column("addr_id"), ForeignKey(typeof(DbEntityAddress), nameof(DbEntityAddress.Key))]
        public override Guid SourceKey
        {
            get;
            set;
        }
    }
}