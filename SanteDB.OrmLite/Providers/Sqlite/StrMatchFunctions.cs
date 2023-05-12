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
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// PostgreSQL LEFT() function
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class SqliteSubstringFunction : IDbFilterFunction
    {
        /// <summary>
        /// Get the name for the function
        /// </summary>
        public string Name => "substr";

        /// <summary>
        /// Provider 
        /// </summary>
        public string Provider => SqliteProvider.InvariantName;

        /// <summary>
        /// Create the SQL for first
        /// </summary>
        public SqlStatementBuilder CreateSqlStatement(SqlStatementBuilder current, string filterColumn, string[] parms,
            string operand, Type type)
        {
            var match = Constants.ExtractFilterOperandRegex.Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op))
            {
                op = "=";
            }

            switch (parms.Length)
            {
                case 1:
                    return current.Append(
                        $"substring({filterColumn}, {parms[0]}) {op} substring(?, {parms[0]})",
                        QueryBuilder.CreateParameterValue(value, type));
                case 2:
                    return current.Append(
                        $"substring({filterColumn}, {parms[0]} , {parms[1]}) {op} substring(? , {parms[0]} , {parms[1]})",
                        QueryBuilder.CreateParameterValue(value, type));
            }

            return current.Append($"substring({filterColumn}, {parms[0]}) {op} substring(?, {parms[0]})",
                QueryBuilder.CreateParameterValue(value, type));
        }
    }

    /// <summary>
    /// PostgreSQL RIGHT() function
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class SqliteLastFunction : IDbFilterFunction
    {
        /// <summary>
        /// Get the name for the function
        /// </summary>
        public string Name => "last";

        /// <summary>
        /// Provider    
        /// </summary>
        public string Provider => SqliteProvider.InvariantName;

        /// <summary>
        /// Create the SQL statement
        /// </summary>
        public SqlStatementBuilder CreateSqlStatement(SqlStatementBuilder current, string filterColumn, string[] parms,
            string operand, Type type)
        {
            var match = Constants.ExtractFilterOperandRegex.Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op))
            {
                op = "=";
            }

            return current.Append($"substring({filterColumn}, length({filterColumn}) - {parms[0]} + 1) {op} substring(?, length(?) - {parms[0]} + 1)",
                QueryBuilder.CreateParameterValue(value, type), QueryBuilder.CreateParameterValue(value, type));
        }
    }

    /// <summary>
    /// PostgreSQL RIGHT() function
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class PostgresNocaseFunction : IDbFilterFunction
    {
        /// <summary>
        /// Get the name for the function
        /// </summary>
        public string Name => "nocase";

        /// <summary>
        /// Provider 
        /// </summary>
        public string Provider => SqliteProvider.InvariantName;

        /// <summary>
        /// Create the SQL statement
        /// </summary>
        public SqlStatementBuilder CreateSqlStatement(SqlStatementBuilder current, string filterColumn, string[] parms,
            string operand, Type type)
        {
            return current.Append($"LOWER({filterColumn}) = LOWER(?)",
                QueryBuilder.CreateParameterValue(operand, type));
        }
    }
}