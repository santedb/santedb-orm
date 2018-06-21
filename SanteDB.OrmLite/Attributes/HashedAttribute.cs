using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite.Attributes
{
    /// <summary>
    /// Indicates that a field is hashed before queried
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class HashedAttribute : Attribute
    {
    }
}
