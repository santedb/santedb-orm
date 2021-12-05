﻿using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using SanteDB.Persistence.Data.ADO.Data.Model.Extensibility;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Acts
{
    /// <summary>
    /// Represents a table which can store act data
    /// </summary>
    [Table("act_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbAct : DbIdentified
    {

        /// <summary>
        /// Gets or sets the template
        /// </summary>
        [Column("tpl_id"), ForeignKey(typeof(DbTemplateDefinition), nameof(DbTemplateDefinition.Key))]
        public Guid TemplateKey { get; set; }

        /// <summary>
        /// Identifies the class concept
        /// </summary>
        [Column("cls_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid ClassConceptKey { get; set; }

        /// <summary>
        /// Gets or sets the mood of the act
        /// </summary>
        [Column("mod_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid MoodConceptKey { get; set; }

        /// <summary>
        /// Gets or sets the key
        /// </summary>
        [Column("act_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }
    }

}
