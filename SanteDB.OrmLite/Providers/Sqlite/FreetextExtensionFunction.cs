/*
 * Copyright (C) 2021 - 2024, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 */
using System;
using System.Data;
using System.Linq;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// Freetext extension function
    /// </summary>
    public class FreetextExtensionFunction : IDbInitializedFilterFunction
    {

        // Has spellfix?
        private static bool? m_hasSpellFix;

        // Has soundex?
        private static bool? m_hasSoundex; 

        /// <summary>
        /// Gets the provider
        /// </summary>
        public string Provider => SqliteProvider.InvariantName;

        /// <summary>
        /// Gets the name of the function
        /// </summary>
        public string Name => SanteDB.Core.Model.Query.FilterExtension.FreetextQueryFilterExtension.FilterName;

        /// <inheritdoc />
        public int Order => -100;

        /// <summary>
        /// Create the SQL statement for the extension function
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
                            break;
                        default:
                            throw new InvalidOperationException("SQLite does not understand freetext search on this type of data");
                    }

                    bool needsJoiner = false;
                    foreach (var t in terms)
                    {
                        switch (t.ToLowerInvariant())
                        {
                            case "and":
                            case "&":
                                current.Append(" intersect ");
                                needsJoiner = false;
                                break;
                            case "or":
                            case "|":
                                current.Append(" union ");
                                needsJoiner = false;
                                break;
                            case "not":
                            case "!":
                                current.Append(" except ");
                                needsJoiner = false;
                                break;
                            default:
                                if(needsJoiner)
                                {
                                    current.Append(" intersect ");
                                    needsJoiner = false;
                                }

                                current.Append($"SELECT ent_id FROM FT_ENT_SYSVW WHERE ");

                                bool useApprox = t.StartsWith("~");
                                var term = t;
                                if(useApprox) { term = term.Substring(1); }

                                current.Append("(").Append("LOWER(term) LIKE ?", QueryBuilder.CreateParameterValue($"%{term.ToLowerInvariant()}%", typeof(String)));

                                if (useApprox && m_hasSpellFix.GetValueOrDefault())
                                {
                                    current.Or("editdist3(LOWER(term), ?) < 2", QueryBuilder.CreateParameterValue(term.ToLowerInvariant(), typeof(String)));
                                }
                                if(useApprox && m_hasSoundex.GetValueOrDefault())
                                {
                                    current.Or("soundex(term) = soundex(?)", QueryBuilder.CreateParameterValue(term.ToLowerInvariant(), typeof(String)));
                                }
                                current.Append(")");
                                needsJoiner = true;
                                break;
                        }
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

        /// <inheritdoc />
        public bool Initialize(IDbConnection connection, IDbTransaction transaction) => connection.CheckAndLoadSpellfix();
    }
}
