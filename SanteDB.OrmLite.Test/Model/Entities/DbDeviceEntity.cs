using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;
using System;



namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents the entity representation of an object
    /// </summary>
    [Table("dev_ent_tbl")]
	public class DbDeviceEntity : DbEntitySubTable
    {

		/// <summary>
		/// Gets or sets the security device identifier.
		/// </summary>
		/// <value>The security device identifier.</value>
		[Column("sec_dev_id"), ForeignKey(typeof(DbSecurityDevice), nameof(DbSecurityDevice.Key))]
		public Guid SecurityDeviceKey {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the name of the manufacturer model.
		/// </summary>
		/// <value>The name of the manufacturer model.</value>
		[Column("mnf_name")]
		public string ManufacturerModelName {
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the name of the operating system.
		/// </summary>
		/// <value>The name of the operating system.</value>
		[Column("os_name")]
		public String OperatingSystemName {
			get;
			set;
		}
	}
}

