/*
 * Copyright (C) 2021 - 2021, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-8-5
 */
using System;
using System.Text.RegularExpressions;
using System.Xml;

namespace SanteDB.OrmLite.Providers.Postgres
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
        public string Provider => "pgsql";

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
                        unit = "mon";
                        break;
                    case "d":
                        unit = "day";
                        break;
                    case "w":
                        unit = "weeks";
                        break;
                    case "h":
                        unit = "hours";
                        break;
                    case "m":
                        unit = "mins";
                        break;
                    case "s":
                        unit = "secs";
                        break;
                }
                return current.Append($"GREATEST({filterColumn}::TIMESTAMP - ?::TIMESTAMP, ?::TIMESTAMP - {filterColumn}::TIMESTAMP) {op} '{qty} {unit}'::INTERVAL", QueryBuilder.CreateParameterValue(parms[0], operandType), QueryBuilder.CreateParameterValue(parms[0], operandType));
            }
            else if (TimeSpan.TryParse(value, out TimeSpan timespan))
            {
                return current.Append($"GREATEST({filterColumn}::TIMESTAMP - ?::TIMESTAMP, ?::TIMESTAMP - {filterColumn}::TIMESTAMP) {op} '{timespan.TotalSeconds} secs'::INTERVAL", QueryBuilder.CreateParameterValue(parms[0], operandType), QueryBuilder.CreateParameterValue(parms[0], operandType));
            }
            else
            {
                try
                {
                    // Try to parse as ISO date
                    timespan = XmlConvert.ToTimeSpan(value);
                    return current.Append($"GREATEST({filterColumn}::TIMESTAMP - ?::TIMESTAMP, ?::TIMESTAMP - {filterColumn}::TIMESTAMP) {op} '{timespan.TotalSeconds} secs'::INTERVAL", QueryBuilder.CreateParameterValue(parms[0], operandType), QueryBuilder.CreateParameterValue(parms[0], operandType));
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
        public string Provider => "pgsql";

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
                    return current.Append($"GREATEST({filterColumn}::TIMESTAMP - ?::TIMESTAMP, ?::TIMESTAMP - {filterColumn}::TIMESTAMP) {op} '{timespan.TotalSeconds} secs'::INTERVAL", QueryBuilder.CreateParameterValue(parms[0], operandType));
                else
                    return current.Append($"GREATEST({filterColumn}::TIMESTAMP - CURRENT_TIMESTAMP, CURRENT_TIMESTAMP - {filterColumn}::TIMESTAMP) {op} '{timespan.TotalSeconds} secs'::INTERVAL");

            }
            else
            {
                try
                {
                    // Try to parse as ISO date
                    timespan = XmlConvert.ToTimeSpan(value);

                    if (parms.Length == 1)
                        return current.Append($"GREATEST({filterColumn}::TIMESTAMP - ?::TIMESTAMP, ?::TIMESTAMP - {filterColumn}::TIMESTAMP) {op} '{timespan.TotalSeconds} secs'::INTERVAL", QueryBuilder.CreateParameterValue(parms[0], operandType));
                    else
                        return current.Append($"GREATEST({filterColumn}::TIMESTAMP - CURRENT_TIMESTAMP, CURRENT_TIMESTAMP - {filterColumn}::TIMESTAMP) {op} '{timespan.TotalSeconds} secs'::INTERVAL");
                }
                catch
                {
                    throw new InvalidOperationException("Age needs to have whole number distance and single character unit or be a valid TimeSpan");
                }
            }
        }

    }
}
