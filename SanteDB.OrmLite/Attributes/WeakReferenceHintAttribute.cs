using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Attributes
{
    /// <summary>
    /// Identifies a reference to another table where this data can be located based on the CastAs
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class WeakReferenceHintAttribute : TableReferenceAttribute
    {
        /// <summary>
        /// Create a weak reference table hint
        /// </summary>
        /// <param name="table">The table to where the reference points</param>
        /// <param name="column">The column in the other table</param>
        public WeakReferenceHintAttribute(Type table, string column) : base(table, column)
        {
        }
    }
}
