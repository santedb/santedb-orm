using SanteDB.Core.Model.Constants;
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using SanteDB.Persistence.Data.ADO.Data.Model.Entities;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Roles
{
    /// <summary>
    /// Represents a patient in the SQLite store
    /// </summary>
    [Table("pat_tbl")]
	public class DbPatient : DbPersonSubTable
	{

        /// <summary>
        /// Parent key
        /// </summary>
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
		/// Gets or sets the deceased date.
		/// </summary>
		/// <value>The deceased date.</value>
		[Column("dcsd_utc")]
		public DateTime? DeceasedDate {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the deceased date precision.
		/// </summary>
		/// <value>The deceased date precision.</value>
		[Column("dcsd_prec")]
		public string DeceasedDatePrecision {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the multiple birth order.
		/// </summary>
		/// <value>The multiple birth order.</value>
		[Column("mb_ord")]
		public int? MultipleBirthOrder {
			get;
			set;
		}

	}
}

