using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SanteDB.OrmLite.Providers.Firebird
{
    /// <summary>
    /// Freetext extension function for FirebirdSQL
    /// </summary>
    /// <remarks>This isn't really a full text search - but it does provide a basic set of functions for searching in a free-text like manner</remarks>
    public class FirebirdFreetextExtensionFunction : IDbFilterFunction
    {
        /// <summary>
        /// Gets the provider 
        /// </summary>
        public string Provider => FirebirdSQLProvider.InvariantName;

        /// <summary>
        /// Gets the name of the freetext extension function
        /// </summary>
        public string Name => SanteDB.Core.Model.Query.FilterExtension.FreetextQueryFilterExtension.FilterName;

        /// <summary>
        /// Create a SQL statement for the posrg
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            if (parms.Length == 1)
            {
                if (!String.IsNullOrEmpty(parms[0]))
                {
                    var terms = parms[0].Split(' ').Select(o=>o.Replace("\"", "").ToLowerInvariant()).Where(o=>!"and".Equals(o.Trim(), StringComparison.CurrentCultureIgnoreCase)).ToArray();
                    current.Append($"{filterColumn} IN (");
                    switch (filterColumn.Split('.').Last())
                    {
                        case "ent_id": // entity search
                        case "src_ent_id":
                        case "trg_ent_id":
                            current.Append($"SELECT ent_id FROM ft_ent_systbl WHERE term LIKE ?", QueryBuilder.CreateParameterValue($"%{terms[0]}%", typeof(String)));
                            break;
                        default:
                            throw new InvalidOperationException("PostgreSQL does not understand freetext search on this type of data");
                    }
                    current.Append(")");
                    return current;
                }
                else
                {
                    return current.Append($"{filterColumn} IS NULL"); // Return no results
                }
            }
            else
            {
                throw new InvalidOperationException("Freetext requires a parameter");
            }
        }
    }
}
