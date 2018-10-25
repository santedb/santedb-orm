using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite.Attributes
{
    /// <summary>
    /// Indicates that a field should not be selected
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class SecretAttribute : System.Attribute
    {
    }
}
