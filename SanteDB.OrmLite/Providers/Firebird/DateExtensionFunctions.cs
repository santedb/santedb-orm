/*
 * Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
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
 * Date: 2021-2-9
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;

namespace SanteDB.OrmLite.Providers.Firebird
{

    /// <summary>
    /// Diff function
    /// </summary>
    /// <example>
    /// ?dateOfBirth=:(diff|2018-01-01)&lt;3w
    /// </example>
    public class PostgreDateDiffFunction : IDbFilterFunction
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
    /// Age function
    /// </summary>
    /// <example>
    /// ?dateOfBirth=:(age)&lt;P3Y2DT4H2M
    /// </example>
    public class PostgreAgeFunction : IDbFilterFunction
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
                if(parms.Length == 1)
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
