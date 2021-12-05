using SanteDB.Core.Model.Constants;
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Acts
{
    /// <summary>
    /// Represents storage class for a patient encounter
    /// </summary>
    [Table("pat_enc_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbPatientEncounter : DbActSubTable
    {

        /// <summary>
        /// Parent key
        /// </summary>
        [JoinFilter(PropertyName = nameof(DbAct.ClassConceptKey), Value = ActClassKeyStrings.Encounter)]
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
        /// Identifies the manner in which the patient was discharged
        /// </summary>
        [Column("dsch_dsp_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid DischargeDispositionKey { get; set; }
    }
}
