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
 * Date: 2022-5-30
 */
using System;
using System.Text.RegularExpressions;
using System.Xml;

namespace SanteDB.OrmLite.Providers.Firebird
{


    /// <summary>
    /// Date truncation function compares two dates based on a truncation
    /// </summary>
    /// <example><code>?dateOfBirth=:(date_trunc|M)1975-04-03</code></example>
    public class FirebirdDateTruncFunction : IDbFilterFunction
    {
        /// <summary>
        /// Get the provider
        /// </summary>
        public string Provider => "FirebirdSQL";

        /// <summary>
        /// Get the name 
        /// </summary>
        public string Name => "date_trunc";

        /// <summary>
        /// Create SQL statement
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = new Regex(@"^([<>]?=?)(.*?)$").Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op)) op = "=";
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
                        return current.Append($"{filterColumn} BETWEEN ? AND ?", dtValue.Date, dtValue.Date.AddHours(23).AddMinutes(59).AddSeconds(59) );
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
    /// Provides a date-difference function for Firebird
    /// </summary>
    /// <remarks>This class or filter function converts the <c>date_diff</c> expression tree statement to IBSQL equivalent</remarks>
    /// <example><code>
    /// ?dateOfBirth=:(date_diff|2018-01-01)&lt;3w
    /// </code>
    /// </example>
    public class FirebirdDateDiffFunction : IDbFilterFunction
    {
        /// <summary>
        /// Get the name for the function
        /// </summary>
        public string Name => "date_diff";

        /// <summary>
        /// Provider
        /// </summary>
        public string Provider => "FirebirdSQL";

        /// <summary>
        /// Create the SQL statement
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = new Regex(@"^([<>]?=?)(.*?)$").Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op)) op = "=";

            match = new Regex(@"^(\d*?)([yMdwhms])$").Match(value);
            if (match.Success)
            {
                String qty = match.Groups[1].Value,
                    unit = match.Groups[2].Value;

                switch (unit)
                {
                    case "y":
                        unit = "year";
                        break;

                    case "M":
                        unit = "month";
                        break;

                    case "d":
                        unit = "day";
                        break;

                    case "h":
                        unit = "hour";
                        break;

                    case "m":
                        unit = "minute";
                        break;

                    case "s":
                        unit = "second";
                        break;
                }
                return current.Append($"ABS(DATEDIFF({unit}, {filterColumn}, cast(? as TIMESTAMP))) {op} ?", QueryBuilder.CreateParameterValue(parms[0], operandType), QueryBuilder.CreateParameterValue(parms[0], typeof(Int32)));
            }
            else if (TimeSpan.TryParse(value, out TimeSpan timespan))
            {
                return current.Append($"ABS(DATEDIFF(millisecond, {filterColumn}, cast(? as TIMESTAMP))) {op} ?", QueryBuilder.CreateParameterValue(parms[0], operandType), timespan.TotalMilliseconds);
            }
            else
            {
                try
                {
                    // Try to parse as ISO date
                    timespan = XmlConvert.ToTimeSpan(value);
                    return current.Append($"ABS(DATEDIFF(millisecond, {filterColumn}, cast(? as TIMESTAMP))) {op} ?", QueryBuilder.CreateParameterValue(parms[0], operandType), timespan.TotalMilliseconds);
                }
                catch
                {
                    throw new InvalidOperationException("Date difference needs to have whole number distance and single character unit or be a valid TimeSpan");
                }
            }
        }
    }

    /// <summary>
    /// Firebird Age Function
    /// </summary>
    /// <remarks>This method is an <see cref="IDbFilterFunction"/> which converts the <c>age</c> HDSI expression tree
    /// expression in IBSQL</remarks>
    /// <example><code>
    /// ?dateOfBirth=:(age)&lt;P3Y2DT4H2M</code>
    /// </example>
    public class FirebirdAgeFunction : IDbFilterFunction
    {
        /// <summary>
        /// Get the name for the function
        /// </summary>
        public string Name => "age";

        /// <summary>
        /// Provider
        /// </summary>
        public string Provider => "FirebirdSQL";

        /// <summary>
        /// Create the SQL statement
        /// </summary>
        public SqlStatement CreateSqlStatement(SqlStatement current, string filterColumn, string[] parms, string operand, Type operandType)
        {
            var match = new Regex(@"^([<>]?=?)(.*?)$").Match(operand);
            String op = match.Groups[1].Value, value = match.Groups[2].Value;
            if (String.IsNullOrEmpty(op)) op = "=";

            if (TimeSpan.TryParse(value, out TimeSpan timespan))
            {
                if (parms.Length == 1)
                    return current.Append($"ABS(DATEDIFF(millisecond, {filterColumn}, cast(? as TIMESTAMP))) {op} {timespan.TotalSeconds}", QueryBuilder.CreateParameterValue(parms[0], operandType));
                else
                    return current.Append($"ABS(DATEDIFF(millisecond, {filterColumn}, CURRENT_TIMESTAMP))) {op} {timespan.TotalSeconds}");
            }
            else
            {
                try
                {
                    // Try to parse as ISO date
                    timespan = XmlConvert.ToTimeSpan(value);

                    if (parms.Length == 1)
                        return current.Append($"ABS(DATEDIFF(millisecond, {filterColumn}, cast(? as TIMESTAMP))) {op} {timespan.TotalSeconds}", QueryBuilder.CreateParameterValue(parms[0], operandType));
                    else
                        return current.Append($"ABS(DATEDIFF(millisecond, {filterColumn}, CURRENT_TIMESTAMP))) {op} {timespan.TotalSeconds}");
                }
                catch
                {
                    throw new InvalidOperationException("Age needs to have whole number distance and single character unit or be a valid TimeSpan");
                }
            }
        }
    }
}