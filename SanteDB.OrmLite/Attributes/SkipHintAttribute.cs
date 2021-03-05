using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Attributes
{
    /// <summary>
    /// The skip hint attribute allows the more complex auto-joining 
    /// tools to understand when skipping a join can be performed
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class SkipHintAttribute : Attribute
    {

        /// <summary>
        /// Skip attribute
        /// </summary>
        public SkipHintAttribute(string queryHint)
        {
            this.QueryHint = queryHint;
        }

        /// <summary>
        /// Gets the query path which , if not present in the query, indicates the class can be skipped
        /// </summary>
        public String QueryHint { get; }
    }
}
