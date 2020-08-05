/*
 * Based on OpenIZ, Copyright (C) 2015 - 2019 Mohawk College of Applied Arts and Technology
 * Copyright (C) 2019 - 2020, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
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
 * Date: 2019-11-27
 */
using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// PostgreSQL LEFT() function
    /// </summary>
    public class PostgresSubstringFunction : IDbFilterFunction
    {

        /// <summary>
        /// Get the name for the function
        /// </summary>
        public string Name => "substr";

        /// <summary>
        /// Provider 
        /// </summary>
        public string Provider => "pgsql";

        /// <summary>
        /// Create the SQL for first
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type type)
        {
            var match = new Regex(@"^([<>]?=?)(.*?)$").Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op)) op = "=";

            switch (parms.Length)
            {
                case 1:
                    return current.Append($"SUBSTRING({filterColumn} FROM {parms[0]}) {op} SUBSTRING(? FROM {parms[0]})", QueryBuilder.CreateParameterValue(value, type));
                case 2:
                    return current.Append($"SUBSTRING({filterColumn} FROM {parms[0]} FOR {parms[1]}) {op} SUBSTRING(? FROM {parms[0]} FOR {parms[1]})", QueryBuilder.CreateParameterValue(value, type));
            }
            return current.Append($"SUBSTRING({filterColumn}, {parms[0]}) {op} LEFT(?, {parms[0]})", QueryBuilder.CreateParameterValue(value, type));
        }
    }

    /// <summary>
    /// PostgreSQL RIGHT() function
    /// </summary>
    public class PostgresLastFunction : IDbFilterFunction
    {
        /// <summary>
        /// Get the name for the function
        /// </summary>
        public string Name => "last";

        /// <summary>
        /// Provider 
        /// </summary>
        public string Provider => "pgsql";

        /// <summary>
        /// Create the SQL statement
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type type)
        {
            var match = new Regex(@"^([<>]?=?)(.*?)$").Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op)) op = "=";
            return current.Append($"RIGHT({filterColumn}, {parms[0]}) {op} RIGHT(?, {parms[0]})", QueryBuilder.CreateParameterValue(value, type));
        }

    }

    /// <summary>
    /// Postgrsql string difference function
    /// </summary>
    public class PostgresLevenshteinFunction : IDbFilterFunction
    {
        /// <summary>
        /// Gets thje provider name
        /// </summary>
        public string Provider => "pgsql";

        /// <summary>
        /// Gets the name of the filter
        /// </summary>
        public string Name => "levenshtein";

        /// <summary>
        /// Apply the filter
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = new Regex(@"^([<>]?=?)(.*?)$").Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op)) op = "=";

            switch (parms.Length)
            {
                case 1:
                    return current.Append($"levenshtein(TRIM(LOWER({filterColumn})), TRIM(LOWER(?))) {op} ?", QueryBuilder.CreateParameterValue(parms[0], operandType), QueryBuilder.CreateParameterValue(value, typeof(Int32)));
                case 4: // with insert, delete and substitute costs
                    return current.Append($"levenshtein(TRIM(LOWER({filterColumn})), TRIM(LOWER(?)), {String.Join(",", parms.Skip(1))}) {op} ?", QueryBuilder.CreateParameterValue(parms[0], operandType), QueryBuilder.CreateParameterValue(value, typeof(Int32)));
                default:
                    throw new ArgumentOutOfRangeException("Invalid number of parameters of string diff");
            }
        }
    }
    /// <summary>
    /// Represents the PostgreSQL soundex function
    /// </summary>
    /// <example>
    /// ?name.component.value=:(soundex)Fyfe
    /// or
    /// ?name.component.value=:(soundex|Fyfe)&lt;3
    /// </example>
    public class PostgresSoundexFunction : IDbFilterFunction
    {
        /// <summary>
        /// Gets the name of the function
        /// </summary>
        public string Name => "soundex";

        /// <summary>
        /// Provider 
        /// </summary>
        public string Provider => "pgsql";

        /// <summary>
        /// Creates the SQL statement
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = new Regex(@"^([<>]?=?)(.*?)$").Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op)) op = "=";

            if (parms.Length == 1) // There is a threshold
                return current.Append($"difference({filterColumn}, ?) {op} ?", QueryBuilder.CreateParameterValue(parms[0], operandType), QueryBuilder.CreateParameterValue(value, operandType));
            else
                return current.Append($"soundex({filterColumn}) {op} soundex(?)", QueryBuilder.CreateParameterValue(value, operandType));
        }
    }

    /// <summary>
    /// Represents the PostgreSQL soundex function
    /// </summary>
    public class PostgresMetaphoneFunction : IDbFilterFunction
    {
        /// <summary>
        /// Gets the name of the function
        /// </summary>
        public string Name => "metaphone";

        /// <summary>
        /// Provider 
        /// </summary>
        public string Provider => "pgsql";

        /// <summary>
        /// Creates the SQL statement
        /// </summary>
        /// <example>
        /// ?name.component.value=:(metaphone)Justin
        /// or
        /// ?name.component.value=:(metaphone|5)Hamilton
        /// </example>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = new Regex(@"^([<>]?=?)(.*?)$").Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op)) op = "=";

            if (op != "=") // There is a threshold
                return current.Append($"metaphone({filterColumn}, {parms[0]}) {op} metaphone(?, {parms[0]})", QueryBuilder.CreateParameterValue(value, operandType));
            else
                return current.Append($"metaphone({filterColumn}, 4) {op} metaphone(?, 4)", QueryBuilder.CreateParameterValue(value, operandType));
        }
    }

    /// <summary>
    /// Represents the PostgreSQL soundex function
    /// </summary>
    public class PostgresDoubleMetaphoneFunction : IDbFilterFunction
    {
        /// <summary>
        /// Gets the name of the function
        /// </summary>
        public string Name => "dmetaphone";

        /// <summary>
        /// Provider 
        /// </summary>
        public string Provider => "pgsql";

        /// <summary>
        /// Creates the SQL statement
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = new Regex(@"^([<>]?=?)(.*?)$").Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op)) op = "=";
            return current.Append($"((dmetaphone({filterColumn}) {op} dmetaphone(?)) OR (dmetaphone_alt({filterColumn}) {op} dmetaphone_alt(?)))", QueryBuilder.CreateParameterValue(value, operandType));
        }
    }

    /// <summary>
    /// Represents the PostgreSQL soundex function
    /// </summary>
    /// <example>
    /// ?name.component.value=:(soundslike|Betty)
    /// ?name.component.value=:(soundslike|Betty,metaphone)
    /// </example>
    public class PostgresSoundslikeFunction : IDbFilterFunction
    {
        /// <summary>
        /// Gets the name of the function
        /// </summary>
        public string Name => "soundslike";

        /// <summary>
        /// Provider 
        /// </summary>
        public string Provider => "pgsql";

        /// <summary>
        /// Creates the SQL statement
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            if (parms.Length == 1)
                return current.Append($"metaphone({filterColumn}, 4) = metaphone(?, 4)", QueryBuilder.CreateParameterValue(parms[0], operandType));
            else
            {
                switch (parms[1])
                {
                    case "metaphone":
                        return current.Append($"metaphone({filterColumn}, 4) = metaphone(?, 4)", QueryBuilder.CreateParameterValue(parms[0], operandType));
                    case "dmetaphone":
                        return current.Append($"((dmetaphone({filterColumn}) = dmetaphone(?)) OR (dmetaphone_alt({filterColumn}) = dmetaphone_alt(?)))", QueryBuilder.CreateParameterValue(parms[0], operandType));
                    case "soundex":
                        return current.Append($"soundex({filterColumn}) = soundex(?)", QueryBuilder.CreateParameterValue(parms[0], operandType));
                    default:
                        throw new NotSupportedException($"Sounds-like algorithm {parms[1]} is not supported");
                }
            }
        }
    }

}
