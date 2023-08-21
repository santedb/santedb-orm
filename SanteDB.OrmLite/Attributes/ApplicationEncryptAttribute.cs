using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Attributes
{

    /// <summary>
    /// Indicates whether the property should use ALE (if supported by the provider and supported by the 
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ApplicationEncryptAttribute : Attribute
    {

        /// <summary>
        /// Gets the unique field name
        /// </summary>
        public String FieldName { get; set; }

        /// <summary>
        /// Create a new instance of the encryption attribute
        /// </summary>
        public ApplicationEncryptAttribute(String fieldName)
        {
            this.FieldName = fieldName;
        }
    }
}
