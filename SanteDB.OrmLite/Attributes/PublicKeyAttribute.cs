using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite.Attributes
{

    /// <summary>
    /// Indicates that the field is the public key for the attribute
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class PublicKeyAttribute : System.Attribute
    {

    }
    /// <summary>
    /// This attribute is used to identify the column as a public key
    /// rather than stored column and identifies the private (integer)
    /// key that should be used to lookup
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class PublicKeyRefAttribute : System.Attribute
    {

        /// <summary>
        /// Creates a cross reference property
        /// </summary>
        public PublicKeyRefAttribute(String privateKey)
        {
            this.LocalKey = privateKey;
        }

        /// <summary>
        /// The private key name in teh current table containing the integer key
        /// </summary>
        public String LocalKey { get; set; }

    }
}
