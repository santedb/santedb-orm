using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Implementers claim to be a <see cref="IDbProvider"/> which can generate and restore the database contents 
    /// to/from a data stream
    /// </summary>
    public interface IDbBackupProvider : IDbProvider
    {
        /// <summary>
        /// Backup the contents of the database this provider represents to <paramref name="backupStream"/>
        /// </summary>
        /// <param name="backupStream">The stream to write the backup to</param>
        /// <returns>True if the stream was backed up</returns>
        /// <remarks>Implementers should ensure that the no further connections are permitted to the database during the course of the backup operation</remarks>
        bool BackupToStream(Stream backupStream);

        /// <summary>
        /// Restore the contents of the database from <paramref name="restoreStream"/> to the database this provider represents
        /// </summary>
        /// <param name="restoreStream">The stream which contains the backup information</param>
        /// <returns>True if the stream was successfully processed</returns>
        bool RestoreFromStream(Stream restoreStream);

    }
}
