using SanteDB.Core.Model.Constants;
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Acts
{

    /// <summary>
    /// Stores data related to an observation act
    /// </summary>
    [Table("obs_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbObservation : DbActSubTable
    {

        /// <summary>
        /// Parent key
        /// </summary>
        [JoinFilter(PropertyName = nameof(DbAct.ClassConceptKey), Value = ActClassKeyStrings.Observation)]
        public override Guid ParentKey
        {
            get
            {
                return base.ParentKey;
            }

            set
            {
                base.ParentKey = value;
            }
        }

        /// <summary>
        /// Gets or sets the interpretation concept
        /// </summary>
        [Column("int_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid InterpretationConceptKey { get; set; }

        /// <summary>
        /// Identifies the value type
        /// </summary>
        [Column("val_typ")]
        public String ValueType { get; set; }

    }

    /// <summary>
    /// Represents additional data related to a quantified observation
    /// </summary>
    [Table("qty_obs_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbQuantityObservation : DbObsSubTable
    {
        
        /// <summary>
        /// Represents the unit of measure
        /// </summary>
        [Column("uom_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid UnitOfMeasureKey { get; set; }

        /// <summary>
        /// Gets or sets the value of the measure
        /// </summary>
        [Column("qty"), NotNull]
        public Decimal Value { get; set; }

        /// <summary>
        /// Gets or sets the value of the measure
        /// </summary>
        [Column("qty_prc")]
        public Decimal? Precision { get; set; }

    }

    /// <summary>
    /// Identifies the observation as a text obseration
    /// </summary>
    [Table("txt_obs_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbTextObservation : DbObsSubTable
    {
        /// <summary>
        /// Gets the value of the observation as a string
        /// </summary>
        [Column("obs_val")]
        public String Value { get; set; }

    }

    /// <summary>
    /// Identifies data related to a coded observation
    /// </summary>
    [Table("cd_obs_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbCodedObservation : DbObsSubTable
    {

        /// <summary>
        /// Gets or sets the concept representing the value of this
        /// </summary>
        [Column("val_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))] 
        public Guid? Value { get; set; }
        
    }
}
