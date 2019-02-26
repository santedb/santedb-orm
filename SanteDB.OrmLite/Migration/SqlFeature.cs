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

        // Invariant name
        private string m_invariant;

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
                retVal.Name = xd.SelectSingleNode("/feature/@id")?.Value ?? "other update";
                retVal.Description = xd.SelectSingleNode("/feature/summary/text()")?.Value ?? "other update";
                retVal.m_checkRange = xd.SelectSingleNode("/feature/@applyRange")?.Value;
                retVal.m_checkSql = xd.SelectSingleNode("/feature/isInstalled/text()")?.Value;
                retVal.m_invariant = xd.SelectSingleNode("/feature/@invariantName")?.Value;

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
        /// Gets the name of the update
        /// </summary>
        public string Name
        {
            get; private set;
        }

        /// <summary>
        /// Gets the check sql
        /// </summary>
        public string GetCheckSql(string invariantName)
        {
            if (String.IsNullOrEmpty(this.m_checkSql))
            {
                var updateRange = this.m_checkRange.Split('-');
                switch (invariantName.ToLower())
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
                            throw new NotSupportedException($"This update provider does not support {invariantName}");
                    default:
                        throw new InvalidOperationException($"This update provider does not support {invariantName}");
                }
            }
            else
                return this.m_checkSql;
        }

        /// <summary>
        /// Get the deployment sql
        /// </summary>
        public string GetDeploySql(string invariantName)
        {
            if (this.m_invariant.ToLower() == invariantName.ToLower())
                return this.m_deploySql;
            else
                return null;
        }
    }
}
