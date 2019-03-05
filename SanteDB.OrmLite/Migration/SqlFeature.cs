/*
 * Copyright 2015-2019 Mohawk College of Applied Arts and Technology
 *
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
 * User: JustinFyfe
 * Date: 2019-2-28
 */
using SanteDB.Core.Configuration.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;

namespace SanteDB.OrmLite.Migration
{
    /// <summary>
    /// Represents an IDataUpdate drawn from a SQL file
    /// </summary>
    public class SqlFeature : IDataFeature
    {

        // Metadata regex
        private static Regex m_metaRegx = new Regex(@"\/\*\*(.*?)\*\/");

        // Deploy sql
        private string m_deploySql;

        // Check SQL
        private string m_checkRange;

        // Check SQL 
        private string m_checkSql;

        // Remarks
        internal string Remarks { get; private set; }

        // Identifier
        public string Id { get; private set; }

        /// <summary>
        /// Gets or sets the url
        /// </summary>
        internal Uri Url { get; private set; }

        /// <summary>
        /// Load the specified stream
        /// </summary>
        public static SqlFeature Load(Stream source)
        {

            var retVal = new SqlFeature();

            // Get deployment sql
            using (var sr = new StreamReader(source))
                retVal.m_deploySql = sr.ReadToEnd();

            var xmlSql = m_metaRegx.Match(retVal.m_deploySql.Replace("\r\n", ""));
            if (xmlSql.Success)
            {
                var xmlText = xmlSql.Groups[1].Value.Replace("*", "");
                XmlDocument xd = new XmlDocument();
                xd.LoadXml(xmlText);
                retVal.Id = xd.SelectSingleNode("/feature/@id")?.Value ?? "0-0";
                retVal.Name = xd.SelectSingleNode("/feature/@name")?.Value ?? "Other";
                retVal.Description = xd.SelectSingleNode("/feature/summary/text()")?.Value ?? "other update";
                retVal.Remarks = xd.SelectSingleNode("/feature/remarks/text()")?.Value ?? "other update";
                retVal.Url = new Uri(xd.SelectSingleNode("/feature/url/text()")?.Value ?? $"http://help.santesuite.org/ops/santedb/fixpatch/{retVal.Id}");
                retVal.m_checkRange = xd.SelectSingleNode("/feature/@applyRange")?.Value;
                retVal.Scope = xd.SelectSingleNode("/feature/@scope")?.Value;
                retVal.m_checkSql = xd.SelectSingleNode("/feature/isInstalled/text()")?.Value;
                retVal.InvariantName = xd.SelectSingleNode("/feature/@invariantName")?.Value;

            }
            else
                throw new InvalidOperationException("Invalid SQL update file");

            return retVal;
        }

        /// <summary>
        /// Gets the description of the update
        /// </summary>
        public string Description
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets or sets the invariant
        /// </summary>
        public string InvariantName { get; set; }

        /// <summary>
        /// Gets the name of the update
        /// </summary>
        public string Name
        {
            get; private set;
        }

        /// <summary>
        /// Gets the scope of the object
        /// </summary>
        public String Scope { get; internal set; }

        /// <summary>
        /// Gets the check sql
        /// </summary>
        public string GetCheckSql()
        {
            if (String.IsNullOrEmpty(this.m_checkSql))
            {
                var updateRange = this.m_checkRange.Split('-');
                switch (this.InvariantName.ToLower())
                {
                    case "npgsql":
                        if (String.IsNullOrEmpty(this.m_checkRange))
                            return "SELECT TRUE";
                        else
                            return $"select not(string_to_array(get_sch_vrsn(), '.')::int[] between string_to_array('{updateRange[0]}','.')::int[] and string_to_array('{updateRange[1]}', '.')::int[])";
                    case "fbsql":
                        if (String.IsNullOrEmpty(this.m_checkRange))
                            return "SELECT true FROM rdb$database";
                        else
                            throw new NotSupportedException($"This update provider does not support {this.InvariantName}");
                    default:
                        throw new InvalidOperationException($"This update provider does not support {this.InvariantName}");
                }
            }
            else
                return this.m_checkSql;
        }

        /// <summary>
        /// Get the deployment sql
        /// </summary>
        public string GetDeploySql()
        {
            return this.m_deploySql;
        }
    }
}
