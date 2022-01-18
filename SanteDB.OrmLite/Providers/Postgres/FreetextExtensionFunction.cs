using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// Freetext extension function
    /// </summary>
    public class FreetextExtensionFunction : IDbFilterFunction
    {
        /// <summary>
        /// Gets the provider
        /// </summary>
        public string Provider => "pgsql";

        /// <summary>
        /// Gets the name of the function
        /// </summary>
        public string Name => "freetext";

        /// <summary>
        /// Create the SQL statement for the extension function
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            
            if(parms.Length == 1)
            {
                switch(filterColumn.Split('.').Last())
                {
                    case "ent_id": // entity search
                    case "src_ent_id":
                    case "trg_ent_id":
                        return current.Append($"{filterColumn} IN (SELECT ent_id FROM ft_ent_systbl WHERE terms @@ websearch_to_tsquery(?))", QueryBuilder.CreateParameterValue(parms[0], typeof(String)));
                    case "act_id": // act search
                    case "src_act_id":
                    case "trg_act_id":
                        return current.Append($"{filterColumn} IN (SELECT act_id FROM ft_act_systbl WHERE terms @@ websearch_to_tsquery(?))", QueryBuilder.CreateParameterValue(parms[0], typeof(String)));
                    case "cd_id": // code search
                    case "src_cd_id": // code search
                    case "trg_cd_id": // code search
                        return current.Append($"{filterColumn} IN (SELECT cd_id FROM ft_cd_systbl WHERE terms @@ websearch_to_tsquery(?))", QueryBuilder.CreateParameterValue(parms[0], typeof(String)));
                    default:
                        throw new InvalidOperationException("PostgreSQL does not understand freetext search on this type of data");
                }
            }
            else
            {
                throw new InvalidOperationException("Freetext requires a parameter");
            }
        }
    }
}
