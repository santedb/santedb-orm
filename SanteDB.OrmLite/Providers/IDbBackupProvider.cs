/*
 * Copyright (C) 2021 - 2024, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
 * Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors
 * Portions Copyright (C) 2015-2018 Mohawk College of Applied Arts and Technology
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. You may 
 * obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations under 
 * the License.
 * 
 * User: fyfej
 * Date: 2024-1-28
 */
using System.IO;

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
