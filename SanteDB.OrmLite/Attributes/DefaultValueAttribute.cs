using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Attributes
{
    /// <summary>
    /// Assign default
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class DefaultValueAttribute : Attribute
    {
        /// <summary>
        /// Gets the default value
        /// </summary>
        public object DefaultValue { get; }

        /// <summary>
        /// Constructor with value
        /// </summary>
        public DefaultValueAttribute(object value)
        {
            this.DefaultValue = value;
        }
    }
}
