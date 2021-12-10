using SanteDB.Core.Model.Constants;
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents a person
    /// </summary>
    [Table("psn_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbPerson : DbEntitySubTable
	{
        /// <summary>
        /// Parent key
        /// </summary>
        [JoinFilter(PropertyName = nameof(DbEntity.ClassConceptKey), Value = EntityClassKeyStrings.Person)]
        [JoinFilter(PropertyName = nameof(DbEntity.ClassConceptKey), Value = EntityClassKeyStrings.Patient)]
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
        /// Gets or sets the gender concept
        /// </summary>
        /// <value>The gender concept.</value>
        [Column("gndr_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid GenderConceptKey
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the date of birth.
        /// </summary>
        /// <value>The date of birth.</value>
        [Column("dob")]
		public DateTime? DateOfBirth {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the date of birth precision.
		/// </summary>
		/// <value>The date of birth precision.</value>
		[Column("dob_prec")]
		public string DateOfBirthPrecision {
			get;
			set;
		}


	}
}

