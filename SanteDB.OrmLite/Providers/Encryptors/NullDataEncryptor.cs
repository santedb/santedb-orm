using SanteDB.OrmLite.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Providers.Encryptors
{
    /// <summary>
    /// Encryption provider that does not do anything
    /// </summary>
    internal class NullDataEncryptor : IDbEncryptor
    {
        public object CreateQueryValue(OrmAleMode aleMode, object decryptedSource) => decryptedSource;

        public bool HasEncryptionMagic(object objectToTest) => false;

        public bool TryDecrypt(object encryptedObject, out object decrypted)
        {
            decrypted = encryptedObject;
            return true;
        }

        public bool TryEncrypt(OrmAleMode aleMode, object unencryptedObject, out object encrypted)
        {
            encrypted = unencryptedObject;
            return true;
        }

        public bool TryGetEncryptionMode(string columnIdentifier, out OrmAleMode aleMode)
        {
            aleMode = OrmAleMode.Off;
            return false;
        }
    }
}
