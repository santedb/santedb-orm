/*
 * Copyright (C) 2021 - 2022, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
 * Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors
 * Portions Copyright (C) 2015-2018 Mohawk College of Applied Arts and Technology
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. You may 
 * obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations under 
 * the License.
 * 
 * User: fyfej
 * Date: 2022-1-12
 */
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
                if (!String.IsNullOrEmpty(parms[0]))
                {
                    switch (filterColumn.Split('.').Last())
                    {
                        case "ent_id": // entity search
                        case "src_ent_id":
                        case "trg_ent_id":
                            return current.Append($"{filterColumn} IN (SELECT ent_id FROM ft_ent_systbl WHERE terms @@ fti_tsquery(?))", QueryBuilder.CreateParameterValue(parms[0], typeof(String)));
                        case "act_id": // act search
                        case "src_act_id":
                        case "trg_act_id":
                            return current.Append($"{filterColumn} IN (SELECT act_id FROM ft_act_systbl WHERE terms @@ fti_tsquery(?))", QueryBuilder.CreateParameterValue(parms[0], typeof(String)));
                        case "cd_id": // code search
                        case "src_cd_id": // code search
                        case "trg_cd_id": // code search
                            return current.Append($"{filterColumn} IN (SELECT cd_id FROM ft_cd_systbl WHERE terms @@ fti_tsquery(?))", QueryBuilder.CreateParameterValue(parms[0].Split(' '), typeof(String)));
                        default:
                            throw new InvalidOperationException("PostgreSQL does not understand freetext search on this type of data");
                    }
                }
                else
                {
                    return current.Append( $"{filterColumn} IS NULL"); // Return no results
                }
            }
            else
            {
                throw new InvalidOperationException("Freetext requires a parameter");
            }
        }
    }
}
