/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-3-10
 */
using System;
using System.Linq;

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
        public SqlStatementBuilder CreateSqlStatement(SqlStatementBuilder current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            if (parms.Length == 1)
            {
                if (!String.IsNullOrEmpty(parms[0]))
                {
                    var terms = parms[0].Split(' ').Select(o => o.Replace("\"", "").ToLowerInvariant()).Where(o => !"and".Equals(o.Trim(), StringComparison.CurrentCultureIgnoreCase)).ToArray();
                    current.Append($"{filterColumn} IN (");
                    switch (filterColumn.Split('.').Last())
                    {
                        case "ent_id": // entity search
                        case "src_ent_id":
                        case "trg_ent_id":
                            current.Append($"SELECT ent_id FROM ft_ent_systbl WHERE term LIKE ?", QueryBuilder.CreateParameterValue($"%{terms[0]}%", typeof(String)));
                            break;
                        default:
                            throw new InvalidOperationException("FirebirdSQL does not understand freetext search on this type of data");
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
