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
 * User: fyfej
 * Date: 2023-8-29
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

        /// <summary>
        /// Gets the provider
        /// </summary>
        public string Provider => SqliteProvider.InvariantName;

        /// <summary>
        /// Gets the name of the function
        /// </summary>
        public string Name => SanteDB.Core.Model.Query.FilterExtension.FreetextQueryFilterExtension.FilterName;

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
                            current.Append($"SELECT ent_id FROM FT_ENT_SYSVW WHERE ");
                            break;
                        default:
                            throw new InvalidOperationException("SQLite does not understand freetext search on this type of data");
                    }

                    foreach (var t in terms[0].Split(' '))
                    {
                        switch (t.ToLowerInvariant())
                        {
                            case "and":
                            case "&":
                                current.Append(" and ");
                                break;
                            case "or":
                            case "|":
                                current.Append(" or ");
                                break;
                            case "not":
                            case "!":
                                current.Append(" not ");
                                break;
                            default:
                                current.Append("(").Append("LOWER(term) LIKE ?", QueryBuilder.CreateParameterValue($"%{terms[0].ToLowerInvariant()}%", typeof(String)));
                                if (m_hasSpellFix.GetValueOrDefault())
                                {
                                    current.Or("editdist3(LOWER(term), ?) < 2", QueryBuilder.CreateParameterValue(terms[0].ToLowerInvariant(), typeof(String)));
                                }
                                current.Append(")");
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

        /// <summary>
        /// Initialize 
        /// </summary>
        public bool Initialize(IDbConnection connection)
        {
            if (!m_hasSpellFix.HasValue)
            {
                try
                {
                    if (connection.ExecuteScalar<Int32>("SELECT sqlite_compileoption_used('SQLITE_ENABLE_LOAD_EXTENSION')") == 1)
                    {
                        try
                        {
                            try
                            {
                                m_hasSpellFix = connection.ExecuteScalar<Int32>("SELECT editdist3('test','test1');") > 0;
                            }
                            catch
                            {
                                connection.LoadExtension("spellfix");
                                m_hasSpellFix = connection.ExecuteScalar<Int32>("SELECT editdist3('test','test1');") > 0;
                            }
                        }
                        catch { m_hasSpellFix = false; }
                    }
                }
                catch
                {
                    m_hasSpellFix = false;
                }
            }
            else if (m_hasSpellFix.GetValueOrDefault())
            {
                connection.LoadExtension("spellfix");

            }
            return true;
        }
    }
}
