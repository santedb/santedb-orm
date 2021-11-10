using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Acts;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using SanteDB.Persistence.Data.ADO.Data.Model.Entities;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model
{
    /// <summary>
    /// Database association
    /// </summary>
    public interface IDbAssociation
    {
        /// <summary>
        /// Gets or sets the source of the association
        /// </summary>
        Guid SourceKey { get; set; }
    }

    /// <summary>
    /// Versioned association
    /// </summary>
    public interface IDbVersionedAssociation : IDbAssociation
    {
        /// <summary>
        /// Gets or sets the version when the relationship is effective
        /// </summary>
        [Column("efft_vrsn_seq_id")]
        Int64 EffectiveVersionSequenceId { get; set; }

        /// <summary>
        /// Gets or sets the verson when the relationship is not effecitve
        /// </summary>
        [Column("obslt_vrsn_seq_id")]
        Int64? ObsoleteVersionSequenceId { get; set; }
    }

    /// <summary>
    /// Represents the databased associated entity
    /// </summary>
    public abstract class DbAssociation : DbIdentified, IDbAssociation
    {
        /// <summary>
        /// Gets or sets the key of the item associated with this object
        /// </summary>
        public abstract Guid SourceKey { get; set; }
    }

    /// <summary>
    /// Represents the versioned copy of an association
    /// </summary>
    public abstract class DbVersionedAssociation : DbAssociation, IDbVersionedAssociation
    {
        /// <summary>
        /// Gets or sets the version when the relationship is effective
        /// </summary>
        [Column("efft_vrsn_seq_id")]
        public Int64 EffectiveVersionSequenceId { get; set; }

        /// <summary>
        /// Gets or sets the verson when the relationship is not effecitve
        /// </summary>
        [Column("obslt_vrsn_seq_id")]
        public Int64? ObsoleteVersionSequenceId { get; set; }
    }

    /// <summary>
    /// Represents an act association
    /// </summary>
    public abstract class DbActAssociation : DbAssociation
    {
        /// <summary>
        /// Gets or sets the source entity id
        /// </summary>
        [Column("act_id"), ForeignKey(typeof(DbAct), nameof(DbAct.Key))]
        public override Guid SourceKey { get; set; }
    }

    /// <summary>
    /// Represents an act association
    /// </summary>
    public abstract class DbActVersionedAssociation : DbVersionedAssociation
    {
        /// <summary>
        /// Gets or sets the source entity id
        /// </summary>
        [Column("act_id"), ForeignKey(typeof(DbAct), nameof(DbAct.Key))]
        public override Guid SourceKey { get; set; }
    }

    /// <summary>
    /// Represents an act association
    /// </summary>
    public abstract class DbEntityAssociation : DbAssociation
    {
        /// <summary>
        /// Gets or sets the source entity id
        /// </summary>
        [Column("ent_id"), ForeignKey(typeof(DbEntity), nameof(DbEntity.Key))]
        public override Guid SourceKey { get; set; }
    }

    /// <summary>
    /// Represents an act association
    /// </summary>
    public abstract class DbEntityVersionedAssociation : DbVersionedAssociation
    {
        /// <summary>
        /// Gets or sets the source entity id
        /// </summary>
        [Column("ent_id"), ForeignKey(typeof(DbEntity), nameof(DbEntity.Key))]
        public override Guid SourceKey { get; set; }
    }

    /// <summary>
    /// Represents an concept association
    /// </summary>
    public abstract class DbConceptVersionedAssociation : DbVersionedAssociation
    {
        /// <summary>
        /// Gets or sets the source entity id
        /// </summary>
        [Column("cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public override Guid SourceKey { get; set; }
    }
}