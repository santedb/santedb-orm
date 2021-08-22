﻿using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Acts;
using SanteDB.Persistence.Data.ADO.Data.Model.Entities;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model
{
    /// <summary>
    /// Gets or sets the derived parent class
    /// </summary>
    public abstract class DbSubTable 
    {

        /// <summary>
        /// Parent key
        /// </summary>
        public abstract Guid ParentKey { get; set; }


    }

    /// <summary>
    /// Act based sub-table
    /// </summary>
    public abstract class DbActSubTable : DbSubTable
    {
        /// <summary>
        /// Gets or sets the parent key
        /// </summary>
        [Column("act_vrsn_id"), ForeignKey(typeof(DbActVersion), nameof(DbActVersion.VersionKey)), PrimaryKey, AlwaysJoin]
        public override Guid ParentKey { get; set; }
    }

    /// <summary>
    /// Act based sub-table
    /// </summary>
    public abstract class DbObsSubTable : DbActSubTable
    {
        /// <summary>
        /// Gets or sets the parent key
        /// </summary>
        [Column("act_vrsn_id"), ForeignKey(typeof(DbObservation), nameof(DbObservation.ParentKey)), PrimaryKey]
        public override Guid ParentKey { get; set; }
    }

    /// <summary>
    /// Entity based sub-table
    /// </summary>
    public abstract class DbEntitySubTable : DbSubTable
    {
        /// <summary>
        /// Gets or sets the parent key
        /// </summary>
        [Column("ent_vrsn_id"), ForeignKey(typeof(DbEntityVersion), nameof(DbEntityVersion.VersionKey)), PrimaryKey, AlwaysJoin]
        public override Guid ParentKey { get; set; }
    }

    /// <summary>
    /// Represents a person based sub-table
    /// </summary>
    public abstract class DbPersonSubTable : DbEntitySubTable
    {
        /// <summary>
        /// Gets or sets the parent key
        /// </summary>
        [Column("ent_vrsn_id"), ForeignKey(typeof(DbPerson), nameof(DbPerson.ParentKey)), PrimaryKey, AlwaysJoin]
        public override Guid ParentKey { get; set; }
    }
}
