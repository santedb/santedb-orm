using SanteDB.Core.Data.Backup;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Configuration;
using SanteDB.OrmLite.Migration;
using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Provides a base class for any ORM based provider which needs backup and restore
    /// </summary>
    public abstract class OrmBackupRestoreServiceBase<TOrmConfiguration>: IRestoreBackupAssets where TOrmConfiguration : OrmConfigurationBase
    {
        
        private readonly IConfigurationManager m_configurationManager;
        private readonly Guid m_assetId;

        /// <summary>
        /// Backup service for audit
        /// </summary>
        public OrmBackupRestoreServiceBase(IConfigurationManager configurationManager, Guid assetId)
        {
            this.m_configurationManager = configurationManager;
            this.m_assetId = assetId;
        }

        /// <inheritdoc/>
        public Guid[] AssetClassIdentifiers => new Guid[] { this.m_assetId };


        /// <inheritdoc/>
        public bool Restore(IBackupAsset backupAsset)
        {
            if (backupAsset == null)
            {
                throw new ArgumentNullException(nameof(backupAsset));
            }
            else if (backupAsset.AssetClassId != m_assetId)
            {
                throw new InvalidOperationException();
            }

            return this.m_configurationManager.GetSection<TOrmConfiguration>().Provider.RestoreBackupAsset(backupAsset);
        }

     
    }
}
