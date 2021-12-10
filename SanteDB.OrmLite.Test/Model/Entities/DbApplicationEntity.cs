using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents an entity which is used to represent an application
    /// </summary>
    [Table("app_ent_tbl")]
    [ExcludeFromCodeCoverage]
	public class DbApplicationEntity : DbEntitySubTable
    {
		/// <summary>
		/// Gets or sets the security application.
		/// </summary>
		/// <value>The security application.</value>
		[Column("sec_app_id"), ForeignKey(typeof(DbSecurityApplication), nameof(DbSecurityApplication.Key))]
		public Guid SecurityApplicationKey {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the name of the software.
		/// </summary>
		/// <value>The name of the software.</value>
		[Column("soft_name")]
		public String SoftwareName {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the name of the version.
		/// </summary>
		/// <value>The name of the version.</value>
		[Column("ver_name")]
		public String VersionName {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the name of the vendor.
		/// </summary>
		/// <value>The name of the vendor.</value>
		[Column("vnd_name")]
		public String VendorName {
			get;
			set;
		}
	}
}

