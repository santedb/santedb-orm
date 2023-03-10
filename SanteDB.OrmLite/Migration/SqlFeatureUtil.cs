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
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Exceptions;
using SanteDB.Core.i18n;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite.Migration
{
    /// <summary>
    /// Represents a utility class for deploying a SQL feature
    /// </summary>
    public static class SqlFeatureUtil
    {
        // Features
        private static IEnumerable<IDataFeature> m_features = null;

        private static readonly Regex sr_SqlLogInstruction = new Regex(@"^.*?--\s?INFO:(.*)$", RegexOptions.Multiline | RegexOptions.Compiled);

        private static Tracer m_traceSource = Tracer.GetTracer(typeof(SqlFeatureUtil));

        /// <summary>
        /// Get all providers
        /// </summary>
        private static IEnumerable<IDataConfigurationProvider> m_providers = null;

        /// <summary>
        /// Get configuration providers
        /// </summary>
        private static IEnumerable<IDataConfigurationProvider> GetConfigurationProviders()
        {
            if (m_providers == null)
            {
                m_providers = AppDomain.CurrentDomain.GetAllTypes()
                    .Where(t => typeof(IDataConfigurationProvider).IsAssignableFrom(t) && !t.IsAbstract && t.IsClass)
                    .Select(t => Activator.CreateInstance(t))
                    .OfType<IDataConfigurationProvider>()
                    .ToList();
            }

            return m_providers;
        }

        /// <summary>
        /// Upgrade schema
        /// </summary>
        public static void UpgradeSchema(this IDbProvider provider, string scopeOfContext) => UpgradeSchema(provider, scopeOfContext, null);

        /// <summary>
        /// Upgrade the schema
        /// </summary>
        public static void UpgradeSchema(this IDbProvider provider, string scopeOfContext, Action<float, string> progressMonitor)
        {
            // First, does the database exist?
            m_traceSource.TraceInfo("Ensure context {0} is updated...", scopeOfContext);
            var configProvider = GetConfigurationProviders().FirstOrDefault(o => o.DbProviderType == provider.GetType());
            var connectionString = new ConnectionString(provider.Invariant, provider.ConnectionString);
            var dbName = connectionString.GetComponent(configProvider.Capabilities.NameSetting);
            // TODO: Move this to a common location
            if (AppDomain.CurrentDomain.GetData("DataDirectory") != null)
            {
                dbName = dbName.Replace("|DataDirectory|", AppDomain.CurrentDomain.GetData("DataDirectory").ToString());
            }
            if (!configProvider.GetDatabases(connectionString).Any(d => d.Equals(System.IO.Path.GetFileName(dbName), StringComparison.OrdinalIgnoreCase)))
            {
                try
                {
                    progressMonitor?.Invoke(0.0f, String.Format(UserMessages.INITIALIZE_DATABASE, scopeOfContext));
                    m_traceSource.TraceInfo("Will create database {0}...", dbName);
                    configProvider.CreateDatabase(connectionString, dbName, connectionString.GetComponent(configProvider.Capabilities.UserNameSetting));
                }
                catch (Exception e)
                {
                    throw new Exception($"Could not initialize databasae {dbName}", e);
                }
            }

            var updates = GetFeatures(provider.Invariant).OfType<SqlFeature>().Where(o => o.Scope == scopeOfContext).OrderBy(o => o.Id).ToArray();

            // Some of the updates from V2 to V3 can take hours to complete - this timer allows us to report progress on the log
            int i = 0;
            foreach (var itm in updates)
            {
                try
                {
                    using (var conn = provider.GetWriteConnection())
                    {
                        progressMonitor?.Invoke((((float)++i) / updates.Length), String.Format(UserMessages.UPDATE_DATABASE, itm.Description));

                        if (!conn.IsInstalled(itm))
                        {
                            m_traceSource.TraceInfo("Installing {0} ({1})...", itm.Id, itm.Description);
                            conn.Install(itm);
                        }
                        else
                        {
                            m_traceSource.TraceInfo("Skipping {0}...", itm.Id);
                        }
                    }
                }
                catch (Exception e)
                {
                    m_traceSource.TraceError("Could not install {0} - {1}", itm.Id, e);
                    throw new DataException($"Could not install {itm.Id}", e);
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
            {
                using (var cmd = conn.Connection.CreateCommand())
                {
                    try
                    {

                        if (String.IsNullOrEmpty(dsql.Trim()))
                        {
                            continue;
                        }

                        var infoLog = sr_SqlLogInstruction.Match(dsql);
                        if (infoLog.Success)
                        {
                            m_traceSource.TraceInfo(infoLog.Groups[1].Value);
                        }
                        cmd.CommandTimeout = 36000;
                        cmd.CommandText = dsql;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandTimeout = 7200; // Default is 2 hrs to upgrade since some schema updates may take several hours to apply
                        m_traceSource.TraceVerbose("EXEC: {0}", dsql);

                        cmd.ExecuteScalar();
                    }
                    catch (Exception e)
                    {
                        if (!cmd.CommandText.Contains("OPTIONAL"))
                        {
#if DEBUG
                            m_traceSource.TraceError("SQL Statement Failed: {0} - {1}", cmd.CommandText, e.Message);
#endif
                            throw new DataPersistenceException($"SQL statement failed {dsql}", e);
                        }
                        else
                        {
                            m_traceSource.TraceWarning("Optional SQL Statement Failed due to {0}: {1}", e.Message, cmd.CommandText);
                        }
                    }
                }
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
            {
                using (var cmd = conn.Connection.CreateCommand())
                {
                    cmd.CommandText = preConditionSql;
                    cmd.CommandType = System.Data.CommandType.Text;
                    if ((bool?)cmd.ExecuteScalar() != true) // can't install
                    {
                        throw new ConstraintException($"Pre-check for {migration.Id} failed");
                    }
                }
            }

            if (!String.IsNullOrEmpty(checkSql))
            {
                using (var cmd = conn.Connection.CreateCommand())
                {
                    cmd.CommandText = checkSql;
                    cmd.CommandType = System.Data.CommandType.Text;
                    return conn.Provider.ConvertValue<bool?>(cmd.ExecuteScalar()) == true;
                }
            }

            return true;
        }

        /// <summary>
        /// Load the available features
        /// </summary>
        public static IEnumerable<IDataFeature> GetFeatures(String invariantName)
        {
            if (m_features == null)
            {
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
                        catch (Exception e)
                        {
                            m_traceSource.TraceError("Could not load {0}: {1}", n, e);
                            return (SqlFeature)null;
                        }
                    })).OfType<IDataFeature>().ToList();
            }

            return m_features.Where(o => o.InvariantName == invariantName);
        }
    }
}