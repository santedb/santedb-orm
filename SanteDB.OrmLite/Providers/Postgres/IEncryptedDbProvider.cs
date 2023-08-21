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
using DocumentFormat.OpenXml.Office2010.ExcelAc;
using SanteDB.OrmLite.Attributes;
using SanteDB.OrmLite.Configuration;
using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// Represents a DB provider that can perform Application Level Encryption
    /// </summary>
    /// <remarks>
    /// <para>
    /// Some database management systems don't have the ability to perform Transparent Data Encryption, this 
    /// marker interface is used so that SanteDB can encrypt data at the application layer as a fallback. There are 
    /// several disadvantages to using ALE, namely that searching must be performed on exact matching and the application
    /// server is responsible for encrypting data to/from the database. This interface allows the ORM components to 
    /// encrypt data using a Symmetric Master Key (SMK) which is encrypted and stored in the database using the 
    /// application master key (AMK) which is on the application server represented by <see cref="EncryptionCertificate"/>.
    /// </para>
    /// <para>
    /// The ORM components then use the <see cref="GetEncryptionProvider()"/> method to encrypt and decrypt any columns which are 
    /// annotated with <see cref="ApplicationEncryptAttribute"/> transaprently from the caller
    /// </para>
    /// </remarks>
    public interface IEncryptedDbProvider : IDbProvider
    {

        /// <summary>
        /// Set the encryption certificate
        /// </summary>
        void SetEncryptionSettings(IOrmEncryptionSettings ormEncryptionSettings);

        /// <summary>
        /// Get the encryption provider
        /// </summary>
        /// <returns></returns>
        IDbEncryptor GetEncryptionProvider();

        /// <summary>
        /// Migrate the encryption
        /// </summary>
        void MigrateEncryption(IOrmEncryptionSettings ormEncryptionSettings);
    }
}