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
 * Date: 2023-8-28
 */
using SanteDB.OrmLite.Configuration;
using System.Security.Cryptography.X509Certificates;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents encryption settings
    /// </summary>
    public interface IOrmEncryptionSettings
    {

        /// <summary>
        /// A seeding value for salts in the database
        /// </summary>
        byte[] SaltSeed { get; }

        /// <summary>
        /// Gets the mode of the ALE
        /// </summary>
        bool AleEnabled { get; }

        /// <summary>
        /// Get the certificate used for encryption
        /// </summary>
        X509Certificate2 Certificate { get; }

        /// <summary>
        /// True if the <paramref name="fieldName"/> is to be encrypted
        /// </summary>
        bool ShouldEncrypt(string fieldName, out OrmAleMode configuredMode);
    }
}