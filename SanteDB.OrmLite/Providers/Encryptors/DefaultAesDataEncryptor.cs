using DocumentFormat.OpenXml.Drawing;
using SanteDB.Core.Configuration;
using SanteDB.Core.i18n;
using SanteDB.OrmLite.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
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

        private static readonly byte[] MAGIC = { (byte)'S', (byte)'B', 0x00, (byte)'A', (byte)'E' };
        private const string MAGIC_STRING = "5342004145";

        // Secret
        private readonly byte[] m_secret;
        private readonly byte[] m_saltSeed;
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
            using (var rsa = encryptionSettings.Certificate.GetRSAPublicKey())
            {
                this.m_saltSeed = rsa.Encrypt(encryptionSettings.SaltSeed, RSAEncryptionPadding.Pkcs1);
            }
        }

        /// <summary>
        /// Generate a master key
        /// </summary>
        public static byte[] GenerateMasterKey(IOrmEncryptionSettings encryptionSettings)
        {
            using (var rsa = encryptionSettings.Certificate.GetRSAPublicKey())
            {
                var byteBuffer = new byte[32];
                RandomNumberGenerator.Create().GetBytes(byteBuffer);
                return rsa.Encrypt(byteBuffer, RSAEncryptionPadding.Pkcs1);
            }
        }

        /// <summary>
        /// Encrypt the data
        /// </summary>
        private byte[] Encrypt(OrmAleMode mode, byte[] data)
        {
            using (var aes = Aes.Create())
            {
                var originalDataHash = MD5.Create().ComputeHash(data);
                switch (mode)
                {
                    case Configuration.OrmAleMode.Random:
                        aes.GenerateIV();
                        break;
                    case Configuration.OrmAleMode.Deterministic:
                        aes.IV = Enumerable.Range(0, originalDataHash.Length).Select(o => (byte)(originalDataHash[o] ^ m_saltSeed[o])).ToArray();
                        break;
                    case Configuration.OrmAleMode.Off:
                        return data;
                }

                aes.Key = this.m_secret;
                using (var encryptor = aes.CreateEncryptor())
                {
                    var encData = encryptor.TransformFinalBlock(data, 0, data.Length);

                    var retVal = new byte[encData.Length + aes.IV.Length + MAGIC.Length];
                    Array.Copy(MAGIC, 0, retVal, 0, MAGIC.Length);

                    Array.Copy(aes.IV, 0, retVal, 5, aes.IV.Length);
                    Array.Copy(encData, 0, retVal, 21, encData.Length);

                    return retVal;
                }
            }
        }

        /// <summary>
        /// Decrypt the data
        /// </summary>
        private byte[] Decrypt(byte[] data)
        {
            try
            {
                using (var aes = Aes.Create())
                {
                    // Does our MAGIC appear?
                    if (!this.HasEncryptionMagic(data))
                    {
                        return data; // not encrypted by US!
                    }
                    var iv = data.Skip(5).Take(16).ToArray();
                    aes.Key = this.m_secret;
                    aes.IV = iv;
                    using (var decryptor = aes.CreateDecryptor())
                    {
                        return decryptor.TransformFinalBlock(data, 21, data.Length - 21);
                    }
                }
            }
            catch (Exception e)
            {
                throw new DataException(ErrorMessages.DECRYPTION_FAILED, e);
            }
        }

        /// <summary>
        /// Create the query value for the object
        /// </summary>
        public object CreateQueryValue(OrmAleMode aleMode, object unencryptedObject)
        {
            switch (aleMode)
            {
                case OrmAleMode.Random:
                    throw new NotSupportedException(ErrorMessages.FILTER_RANDOM_ENCRYPTION_NOT_SUPPORTED);
                case OrmAleMode.Deterministic:
                    if(this.TryEncrypt(aleMode, unencryptedObject, out var retVal))
                    {
                        return retVal;
                    }
                    return unencryptedObject;
                default:
                    return unencryptedObject;
            }
        }

        /// <inheritdoc/>
        public bool TryDecrypt(object encryptedObject, out object decrypted)
        {
            switch (encryptedObject)
            {
                case String s:
                    if (s.IsHexEncoded() && s.StartsWith(MAGIC_STRING))
                    {
                        decrypted = Encoding.UTF8.GetString(this.Decrypt(s.HexDecode()));
                        return true;
                    }
                    decrypted = encryptedObject;
                    return false;
                case byte[] b:
                    decrypted = this.Decrypt(b);
                    return true;
                default:
                    decrypted = encryptedObject;
                    return false;
            }
        }

        /// <inheritdoc/>
        public bool TryEncrypt(OrmAleMode mode, object unencryptedObject, out object encrypted)
        {
            if (mode == OrmAleMode.Off)
            {
                encrypted = unencryptedObject;
                return true;
            }
            else
            {
                switch (unencryptedObject)
                {
                    case String s:
                        encrypted = this.Encrypt(mode, Encoding.UTF8.GetBytes(s)).HexEncode();
                        return true;
                    case byte[] b:
                        encrypted = this.Encrypt(mode, b);
                        return true;
                    default:
                        encrypted = null;
                        return false;
                }
            }
        }

        /// <inheritdoc/>
        public bool HasEncryptionMagic(object encrypted)
        {
            switch (encrypted)
            {
                case string s:
                    return s.StartsWith(MAGIC_STRING, StringComparison.OrdinalIgnoreCase) && s.IsHexEncoded();
                case byte[] b:
                    return b.Take(5).SequenceEqual(MAGIC);
                default:
                    return false;
            }
        }

        /// <inheritdoc/>
        public bool TryGetEncryptionMode(string fieldName, out OrmAleMode configuredMode) => this.m_settings.ShouldEncrypt(fieldName, out configuredMode);
    }
}
