﻿/*
 * Copyright (C) 2021 - 2025, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// ADO data constants to be used in the ADO provider
    /// </summary>
    public static class Constants
    {

        /// <summary>
        /// Represents the trace source name
        /// </summary>
        public const string TracerName = "SanteDB.OrmLite";


        internal const int SQL_GROUP_DISTINCT = 1;
        internal const int SQL_GROUP_COLUMNS = 2;
        internal const int SQL_GROUP_FROM = 3;
        internal const int SQL_GROUP_WHERE = 4;
        internal const int SQL_GROUP_LIMIT = 5;

        public static readonly Regex ExtractColumnBindingRegex = new Regex(@"([A-Za-z_]\w+\.)?([A-Za-z_\*]\w+)(,)?", RegexOptions.Compiled);
        public static readonly Regex ExtractUnionIntersectRegex = new Regex(@"^(.*?)(UNION|INTERSECT|UNION ALL|INTERSECT ALL)(.*?)$", RegexOptions.Compiled);
        public static readonly Regex ExtractRawSqlStatementRegex = new Regex(@"^SELECT\s(DISTINCT)?(.*?)FROM(.*?)(?:WHERE(.*?))?((ORDER|OFFSET|LIMIT).*)?$", RegexOptions.Compiled);
        public static readonly Regex ExtractFilterOperandRegex = new Regex(@"^([<>]?=?)(.*?)$", RegexOptions.Compiled);
        public static readonly Regex ExtractOffsetRegex = new Regex(@"OFFSET (\d+)\s?(?:ROW)?", RegexOptions.Compiled);
        public static readonly Regex ExtractLimitRegex = new Regex(@"(?:FETCH\sFIRST|LIMIT)\s(\d+)(?:\sROWS\sONLY)?", RegexOptions.Compiled);
        public static readonly Regex ExtractOrderByRegex = new Regex(@"^(.*?)(ORDER BY ((.*?) (ASC|DESC)\s*,?){0,})(.*)$", RegexOptions.Compiled);
        public static readonly Regex ExtractCommentsRegex = new Regex(@"(.*?)--.*$", RegexOptions.Multiline | RegexOptions.Compiled);
    }
}
