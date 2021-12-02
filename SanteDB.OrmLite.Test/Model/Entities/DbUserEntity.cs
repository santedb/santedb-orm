using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// User entity ORM
    /// </summary>
    [Table("usr_ent_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbUserEntity : DbPersonSubTable
    {

        /// <summary>
        /// Gets or sets the security user which is associated with this entity
        /// </summary>
        [Column("sec_usr_id"), ForeignKey(typeof(DbSecurityUser), nameof(DbSecurityUser.Key))]
        public Guid SecurityUserKey { get; set; }

    }
}
