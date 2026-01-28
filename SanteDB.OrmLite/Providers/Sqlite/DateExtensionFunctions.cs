/*
 * Copyright (C) 2021 - 2026, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-6-21
 */
using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
using System.Xml;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// Date difference function for SQLite
    /// </summary>
    /// <remarks>This method converts the HDSI <c>date_diff</c> function from the HDSI expression tree
    /// into PL/PGSQL</remarks>
    /// <example><code>
    /// ?dateOfBirth=:(diff|2018-01-01)&lt;3w</code>
    /// </example>
    [ExcludeFromCodeCoverage]
    public class SqliteDateDiffFunction : IDbFilterFunction
    {
        /// <summary>
        /// Date difference name
        /// </summary>
        public string Name => "date_diff";

        /// <inheritdoc/>
        public string Provider => SqliteProvider.InvariantName;

        /// <summary>
        /// Create sql statement
        /// </summary>
        public SqlStatementBuilder CreateSqlStatement(SqlStatementBuilder current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = Constants.ExtractFilterOperandRegex.Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op))
            {
                op = "=";
            }

            match = new Regex(@"^(\d*?)([yMdwhms])$").Match(value);
            if (match.Success)
            {
                String qty = match.Groups[1].Value,
                    unit = match.Groups[2].Value;
                long qtyParse = long.Parse(qty);

                switch (unit)
                {
                    case "y":
                        qtyParse *= 31557600;
                        break;
                    case "M":
                        qtyParse *= 2592000;
                        break;
                    case "w":
                        qtyParse *= 604800;
                        break;
                    case "d":
                        qtyParse *= 86400;
                        break;
                    case "h":
                        qtyParse *= 3600;
                        break;
                    case "m":
                        qtyParse *= 60;
                        break;
                    case "s":
                        qtyParse *= 1;
                        break;
                }
                return current.Append($"ABS({filterColumn} - ?) {op} {qtyParse}", QueryBuilder.CreateParameterValue(parms[0], operandType));
            }
            else if (TimeSpan.TryParse(value, out TimeSpan timespan))
            {
                return current.Append($"ABS({filterColumn} - ?) {op} {timespan.Ticks}", QueryBuilder.CreateParameterValue(parms[0], operandType));
            }
            else
            {
                throw new InvalidOperationException("Date difference needs to have whole number distance and single character unit or be a valid TimeSpan");
            }
        }
    }

    /// <summary>
    /// Date truncation function compares two dates based on a truncation
    /// </summary>
    /// <example><code>?dateOfBirth=:(date_trunc|M)1975-04-03</code></example>
    public class SqliteDateTruncFunction : IDbFilterFunction
    {
        /// <summary>
        /// Get the provider
        /// </summary>
        public string Provider => SqliteProvider.InvariantName;

        /// <summary>
        /// Get the name 
        /// </summary>
        public string Name => "date_trunc";

        /// <summary>
        /// Create SQL statement
        /// </summary>
        public SqlStatementBuilder CreateSqlStatement(SqlStatementBuilder current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = Constants.ExtractFilterOperandRegex.Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op))
            {
                op = "=";
            }

            if (parms.Length == 1) // There is a threshold
            {
                var dtValue = DateTime.Parse(value);
                switch (parms[0].Replace("\"", ""))
                {
                    case "y":
                        return current.Append($"{filterColumn} BETWEEN ? AND ?", new DateTime(dtValue.Year, 01, 01), new DateTime(dtValue.Year, 12, 31, 23, 59, 59));
                    case "M":
                        return current.Append($"{filterColumn} BETWEEN ? AND ?", new DateTime(dtValue.Year, dtValue.Month, 01), new DateTime(dtValue.Year, dtValue.Month, DateTime.DaysInMonth(dtValue.Year, dtValue.Month), 23, 59, 59));
                    case "d":
                        return current.Append($"{filterColumn} BETWEEN ? AND ?", dtValue.Date, dtValue.Date.AddHours(23).AddMinutes(59).AddSeconds(59));
                    default:
                        throw new NotSupportedException("Date truncate precision must be y, M, or d");
                }
            }
            else
            {
                throw new InvalidOperationException("date_trunc requires a precision");
            }
        }
    }

    /// <summary>
    /// Age function for PostgreSQL
    /// </summary>
    /// <remarks>This class converts the <c>age</c> HDSI expression tree into a PL/PGSQL
    /// statement.</remarks>
    /// <example><code>
    /// ?dateOfBirth=:(age)&lt;P3Y2DT4H2M</code>
    /// </example>
    [ExcludeFromCodeCoverage]
    public class SqliteAgeFunction : IDbFilterFunction
    {
        /// <summary>
        /// Get the name for the function
        /// </summary>
        public string Name => "age";

        /// <summary>
        /// Provider
        /// </summary>
        public string Provider => SqliteProvider.InvariantName;

        /// <summary>
        /// Create the SQL statement
        /// </summary>
        public SqlStatementBuilder CreateSqlStatement(SqlStatementBuilder current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = Constants.ExtractFilterOperandRegex.Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op))
            {
                op = "=";
            }

            if (TimeSpan.TryParse(value, out TimeSpan timespan))
            {
                if (parms.Length == 1)
                {
                    return current.Append($"ABS({filterColumn} - ?) {op} {timespan.TotalSeconds}", QueryBuilder.CreateParameterValue(parms[0], operandType));
                }
                else
                {
                    return current.Append($"ABS({filterColumn} - unixepoch()) {op} {timespan.TotalSeconds}");
                }
            }
            else
            {
                try
                {
                    // Try to parse as ISO date
                    timespan = XmlConvert.ToTimeSpan(value);

                    if (parms.Length == 1)
                    {
                        return current.Append($"ABS({filterColumn} - ?) {op} {timespan.TotalSeconds}", QueryBuilder.CreateParameterValue(parms[0], operandType));
                    }
                    else
                    {
                        return current.Append($"ABS({filterColumn} - unixepoch()) {op} {timespan.TotalSeconds}");
                    }
                }
                catch
                {
                    throw new InvalidOperationException("Age needs to have whole number distance and single character unit or be a valid TimeSpan");
                }
            }
        }
    }
}