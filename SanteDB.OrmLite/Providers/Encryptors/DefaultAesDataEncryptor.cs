using DocumentFormat.OpenXml.Drawing;
using SanteDB.Core.Configuration;
using SanteDB.Core.i18n;
using SanteDB.OrmLite.Providers.Postgres;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace SanteDB.OrmLite.Providers.Encryptors
{
    /// <summary>
    /// Default AES based data encryptor
    /// </summary>
    internal class DefaultAesDataEncryptor : IDbEncryptor
    {

        private static readonly byte[] MAGIC = { 0xde, 0xad, 0x00, 0xfe, 0xed };

        // Secret
        private readonly byte[] m_secret;
        private readonly IOrmEncryptionSettings m_settings;

        /// <summary>
        /// Default AES encryptor
        /// </summary>
        public DefaultAesDataEncryptor(IOrmEncryptionSettings encryptionSettings, byte[] aleMasterKey)
        {
            this.m_settings = encryptionSettings;
            using (var rsa = encryptionSettings.Certificate.GetRSAPrivateKey())
            {
                this.m_secret = rsa.Decrypt(aleMasterKey, RSAEncryptionPadding.Pkcs1);
            }
        }

        /// <summary>
        /// Generate a master key
        /// </summary>
        public static byte[] GenerateMasterKey(IOrmEncryptionSettings encryptionSettings)
        {
            using(var rsa = encryptionSettings.Certificate.GetRSAPublicKey())
            {
                var byteBuffer = new byte[32];
                RandomNumberGenerator.Create().GetBytes(byteBuffer);
                return rsa.Encrypt(byteBuffer, RSAEncryptionPadding.Pkcs1);
            }
        }

        /// <summary>
        /// Encrypt the data
        /// </summary>
        private byte[] Encrypt(byte[] data)
        {
            using (var aes = Aes.Create())
            {
                var originalDataHash = MD5.Create().ComputeHash(data);
                if (this.m_settings.Mode == Configuration.OrmAleMode.Random)
                {
                    aes.GenerateIV();
                }
                else
                {
                    aes.IV = originalDataHash;
                }

                aes.Key = this.m_secret;
                using (var encryptor = aes.CreateEncryptor()) {
                    var encData = encryptor.TransformFinalBlock(data, 0, data.Length);

                    var retVal = new byte[encData.Length + aes.IV.Length + MAGIC.Length + (this.m_settings.Mode == Configuration.OrmAleMode.Random ? 16 : 0)];
                    Array.Copy(MAGIC, 0, retVal, 0, MAGIC.Length);

                    if (this.m_settings.Mode == Configuration.OrmAleMode.Random) // to allow queries - we need a consistent hash stored in the output buffer so our queries will be on the first 22 bytes
                    {
                        Array.Copy(originalDataHash, 0, retVal, 5, 16);
                        Array.Copy(aes.IV, 0, retVal, 21, aes.IV.Length);
                        Array.Copy(encData, 0, retVal, 37, encData.Length);
                    }else
                    {
                        Array.Copy(aes.IV, 0, retVal, 5, aes.IV.Length);
                        Array.Copy(encData, 0, retVal, 21, encData.Length);
                    }

                    return retVal;
                }
            }
        }

        /// <summary>
        /// Decrypt the data
        /// </summary>
        private byte[] Decrypt(byte[] data)
        {
            using (var aes = Aes.Create())
            {
                // Does our MAGIC appear?
                if(!this.HasEncryptionMagic(data))
                {
                    return data; // not encrypted by US!
                }
                var dataOffset = this.m_settings.Mode == Configuration.OrmAleMode.Random ? 21 : 5;
                var iv = data.Skip(dataOffset).Take(16).ToArray();
                aes.Key = this.m_secret;
                aes.IV = iv;
                using (var decryptor = aes.CreateDecryptor())
                {
                    return decryptor.TransformFinalBlock(data, dataOffset + 16, data.Length - dataOffset - 16); 
                }
            }
        }

        /// <summary>
        /// Create the query value for the object
        /// </summary>
        public object CreateQueryValue(object unencryptedObject)
        {
            if(this.m_settings.Mode == Configuration.OrmAleMode.Random)
            {
                // TODO: Implement random query filters
                throw new NotSupportedException(ErrorMessages.FILTER_RANDOM_ENCRYPTION_NOT_SUPPORTED);
            }
            else if(this.TryEncrypt(unencryptedObject, out var retVal))
            {
                return retVal;
            }
            else
            {
                return retVal;  
            }
        }

        /// <inheritdoc/>
        public bool TryDecrypt(object encryptedObject, out object decrypted)
        {
            switch(encryptedObject)
            {
                case String s:
                    if (s.IsHexEncoded())
                    {
                        decrypted = Encoding.UTF8.GetString(this.Decrypt(s.HexDecode()));
                        return true;
                    }
                    decrypted = null;
                    return false;
                case byte[] b:
                    decrypted = this.Decrypt(b);
                    return false;
                default:
                    decrypted = encryptedObject;
                    return false;
            }
        }

        /// <inheritdoc/>
        public bool TryEncrypt(object unencryptedObject, out object encrypted)
        {
            switch(unencryptedObject)
            {
                case String s:
                    encrypted = this.Encrypt(Encoding.UTF8.GetBytes(s)).HexEncode();
                    return true;
                case byte[] b:
                    encrypted = this.Encrypt(b);
                    return true;
                default:
                    encrypted = null;
                    return false;
            }
        }

        /// <inheritdoc/>
        public bool HasEncryptionMagic(object encrypted) {
            switch (encrypted) {
                case string s:
                    return s.Length > 10 && s.IsHexEncoded() && s.StartsWith("DEAD00FEED", StringComparison.OrdinalIgnoreCase);
                case byte[] b:
                    return b.Take(5).SequenceEqual(MAGIC);
                default:
                    return false;
            }
        }

        /// <inheritdoc/>
        public bool IsConfiguredForEncryption(string fieldName) => !String.IsNullOrEmpty(fieldName) && this.m_settings.ShouldEncrypt(fieldName);
    }
}
