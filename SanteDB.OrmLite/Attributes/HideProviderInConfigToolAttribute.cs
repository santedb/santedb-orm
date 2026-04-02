using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Attributes
{
    /// <summary>
    /// Instructs the config tool to ignore this <see cref="SanteDB.Core.Configuration.Data.IDataConfigurationProvider"/>. This attribute is not inherited.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = false, Inherited = false)]
    public class HideProviderInConfigToolAttribute : Attribute
    {
        public HideProviderInConfigToolAttribute()
        {
        }
    }
}
