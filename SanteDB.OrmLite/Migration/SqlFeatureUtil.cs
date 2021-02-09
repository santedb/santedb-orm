/*
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
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite.Migration
{
    /// <summary>
    /// Represents a utility class for deploying a SQL feature
    /// </summary>
    public static class SqlFeatureUtil 
    {

        // Features
        private static IEnumerable<IDataFeature> m_features = null;

        private static Tracer m_traceSource = Tracer.GetTracer(typeof(SqlFeatureUtil));


        /// <summary>
        /// Upgrade the schema 
        /// </summary>
        public static void UpgradeSchema(this DataContext conn, string scopeOfContext)
        {
            conn.Open();
            foreach (var itm in GetFeatures(conn.Provider.Invariant).OfType<SqlFeature>().Where(o=>o.Scope == scopeOfContext).OrderBy(o=>o.Id))
            {
                try
                {
                    if (!conn.IsInstalled(itm))
                    {
                        m_traceSource.TraceInfo("Installing {0} ({1})...", itm.Id, itm.Description);
                        conn.Install(itm);
                    }
                    else
                        m_traceSource.TraceInfo("Skipping {0}...", itm.Id);
                }
                catch(Exception e)
                {
                    throw new Exception($"Could not install {itm.Id}", e);
                }
            }
        }

        /// <summary>
        /// Install the specified object
        /// </summary>
        public static bool Install(this DataContext conn, SqlFeature migration)
        {

            conn.Open();

            var stmts = migration.GetDeploySql().Split(new string[] { "--#!" }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var dsql in stmts)
                using (var cmd = conn.Connection.CreateCommand())
                {
                    if (String.IsNullOrEmpty(dsql.Trim()))
                        continue;
                    cmd.CommandText = dsql;
                    cmd.CommandType = CommandType.Text;
                    m_traceSource.TraceVerbose("EXEC: {0}", dsql);
                    cmd.ExecuteNonQuery();
                }

            return true;
        }

        /// <summary>
        /// Returns true if the migration has been installed
        /// </summary>
        public static bool IsInstalled(this DataContext conn, SqlFeature migration)
        {
            conn.Open();

            string checkSql = migration.GetCheckSql(),
                        preConditionSql = migration.GetPreCheckSql();


            if (!String.IsNullOrEmpty(preConditionSql))
                using (var cmd = conn.Connection.CreateCommand())
                {
                    cmd.CommandText = preConditionSql;
                    cmd.CommandType = System.Data.CommandType.Text;
                    if (!(bool)cmd.ExecuteScalar()) // can't install
                        throw new ConstraintException($"Pre-check for {migration.Id} failed");

                }
            if (!String.IsNullOrEmpty(checkSql))
                using (var cmd = conn.Connection.CreateCommand())
                {
                    cmd.CommandText = checkSql;
                    cmd.CommandType = System.Data.CommandType.Text;
                    return (bool)cmd.ExecuteScalar();
                }

            return true;
        }

        /// <summary>
        /// Load the available features
        /// </summary>
        public static IEnumerable<IDataFeature> GetFeatures(String invariantName)
        {
            if (m_features == null)
                m_features = AppDomain.CurrentDomain.GetAssemblies()
                    .Where(a => !a.IsDynamic)
                    .SelectMany(a => a.GetManifestResourceNames().Where(n => n.ToLower().EndsWith(".sql")).Select(n =>
                    {
                        try
                        {
                            var retVal = SqlFeature.Load(a.GetManifestResourceStream(n));
                            retVal.Scope = retVal.Scope ?? a.FullName;
                            return retVal;
                        }
                        catch (Exception e){
                            m_traceSource.TraceError("Could not load {0}: {1}", n, e);
                            return (SqlFeature)null;
                        }
                    })).OfType<IDataFeature>().ToList();
            return m_features.Where(o=>o.InvariantName == invariantName);
        }


    }
}
