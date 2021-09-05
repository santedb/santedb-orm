using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents an organization in the data store
    /// </summary>
    [Table("org_tbl")]
	public class DbOrganization : DbEntitySubTable
	{
		/// <summary>
		/// Gets or sets the industry concept.
		/// </summary>
		/// <value>The industry concept.</value>
		[Column("ind_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
		public Guid IndustryConceptKey {
			get;
			set;
		}

	}
}

