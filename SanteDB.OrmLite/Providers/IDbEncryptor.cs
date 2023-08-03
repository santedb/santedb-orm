/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-5-19
 */
namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents an implementation of a data encryptor (for ALE)
    /// </summary>
    public interface IDbEncryptor
    {

        /// <summary>
        /// Attempt to encrypt the data with the configured encryption setting
        /// </summary>
        /// <param name="unencryptedObject">The data to be encrypted</param>
        /// <param name="encrypted">The encrypted data</param>
        /// <returns>True if the data could be encrypted by the provider</returns>
        bool TryEncrypt(object unencryptedObject, out object encrypted);

        /// <summary>
        /// Attempt to decrypt the data from the database
        /// </summary>
        /// <param name="encryptedObject">The encrypted object from the data reader</param>
        /// <param name="decrypted">The decrypted data</param>
        /// <returns>True if the ALE decription was successful</returns>
        bool TryDecrypt(object encryptedObject, out object decrypted);

        /// <summary>
        /// Create a query parameter value based on the configuration of the encryption
        /// </summary>
        /// <param name="decryptedSource">The decrypted source</param>
        /// <returns>The value of the parmeter suitable for use in queries</returns>
        object CreateQueryValue(object decryptedSource);

        /// <summary>
        /// Tests whether <paramref name="objectToTest"/> is encrypted by looking at the MAGIC which should appear at the start of the object data
        /// </summary>
        /// <param name="objectToTest">The object to test</param>
        /// <returns>True if the object <paramref name="objectToTest"/> is encrypted</returns>
        bool HasEncryptionMagic(object objectToTest);

        /// <summary>
        /// Tests whether <paramref name="columnIdentifier"/> is configured for encryption
        /// </summary>
        /// <param name="columnIdentifier">The identifier of the column</param>
        /// <returns>True if the column was configured for encryption</returns>
        bool IsConfiguredForEncryption(string columnIdentifier);
    }
}