/*
 * Copyright (C) 2021 - 2024, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
using SanteDB.Core;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Data;
using SanteDB.Core.Data.Backup;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Exceptions;
using SanteDB.Core.i18n;
using SanteDB.Core.Model.Roles;
using SanteDB.OrmLite.Attributes;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
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
        /// Get the configuration provider
        /// </summary>
        public static IDataConfigurationProvider GetDataConfigurationProvider(this IDbProvider provider) => GetConfigurationProviders().FirstOrDefault(o => o.Invariant == provider.Invariant);

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
        public static void UpgradeSchema(this IDbProvider provider, string scopeOfContext, Action<string, float, string> progressMonitor)
        {
            // First, does the database exist?
            m_traceSource.TraceInfo("Ensure context {0} is updated...", scopeOfContext);
            var configProvider = GetConfigurationProviders().FirstOrDefault(o => o.DbProviderType == provider.GetType());
            var connectionString = new ConnectionString(provider.Invariant, provider.ConnectionString);
            var dbName = connectionString.GetComponent(configProvider.Capabilities.NameSetting);
            // TODO: Move this to a common location
            if (AppDomain.CurrentDomain.GetData("DataDirectory") != null)
            {
                dbName = dbName.Replace("|DataDirectory|", AppDomain.CurrentDomain.GetData("DataDirectory").ToString()).Replace("\\", System.IO.Path.DirectorySeparatorChar.ToString());
            }
            if (!configProvider.GetDatabases(connectionString).Any(d => d.Equals(System.IO.Path.GetFileName(dbName), StringComparison.OrdinalIgnoreCase)))
            {
                try
                {
                    progressMonitor?.Invoke(nameof(UpgradeSchema), 0.0f, String.Format(UserMessages.INITIALIZE_DATABASE, scopeOfContext));
                    m_traceSource.TraceInfo("Will create database {0}...", dbName);
                    configProvider.CreateDatabase(connectionString, dbName, connectionString.GetComponent(configProvider.Capabilities.UserNameSetting));
                }
                catch (Exception e)
                {
                    throw new Exception($"Could not initialize databasae {dbName}", e);
                }
            }


            // Some of the updates from V2 to V3 can take hours to complete - this timer allows us to report progress on the log
            int i = 0;
            using (var conn = provider.GetWriteConnection())
            {
                UpgradeSchema(conn, scopeOfContext, progressMonitor);
            }

            progressMonitor?.Invoke(nameof(UpgradeSchema), 1f, UserMessages.COMPLETE);
        }

        /// <summary>
        /// Upgrade the context on the specified data context
        /// </summary>
        public static void UpgradeSchema(DataContext conn, string scopeOfContext) => UpgradeSchema(conn, scopeOfContext);

        /// <summary>
        /// Upgrade schema on the specified connection
        /// </summary>
        private static void UpgradeSchema(DataContext conn, string scopeOfContext, Action<string, float, string> progressMonitor = null)
        {
            var updates = GetFeatures(conn.Provider.Invariant).OfType<SqlFeature>().Where(o => o.Scope == scopeOfContext).OrderBy(o => o.Id).ToArray();
            int i = 0;
            foreach (var itm in updates.Where(o => o.EnvironmentType == null || o.EnvironmentType.Contains(ApplicationServiceContext.Current.HostType)))
            {
                try
                {
                    conn.Open();
                    progressMonitor?.Invoke(nameof(UpgradeSchema), (((float)++i) / updates.Length), String.Format(UserMessages.UPDATE_DATABASE, itm.Description));

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
                catch (Exception e)
                {
                    m_traceSource.TraceError("Could not install {0} - {1}", itm.Id, e);
                    throw new DataException($"Could not install {itm.Id}", e);
                }
                finally
                {
                    conn.Close();
                }
            }
        }

        /// <summary>
        /// Perform a full encryption (or decryption) of the ALE fields configured for the specified data context
        /// </summary>
        internal static void AleRecrypt(this IOrmEncryptionSettings ormEncryptionSettings, IEncryptedDbProvider encryptedDbProvider, Action<string, float, string> progressMonitor = null)
        {
            var tracer = Tracer.GetTracer(typeof(SqlFeatureUtil));
            var propertiesToEncrypt = AppDomain.CurrentDomain.GetAllTypes()
                .Where(t => t.HasCustomAttribute<TableAttribute>())
                .SelectMany(t => t.GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                .Where(p => p.HasCustomAttribute<ApplicationEncryptAttribute>() && ormEncryptionSettings.ShouldEncrypt(p.GetCustomAttribute<ApplicationEncryptAttribute>().FieldName, out _)))
                .ToArray();

            using (var context = encryptedDbProvider.GetWriteConnection())
            {
                try
                {
                    context.Open();
                    float percentCompletePerProperty = 1.0f / (float)propertiesToEncrypt.Length,
                        propertiesComplete = 0.0f;

                    using (var tx = context.BeginTransaction())
                    {
                        // Gather a list of all encryption provided settings
                        for (int i = 0; i < propertiesToEncrypt.Length; i++)
                        {
                            var property = propertiesToEncrypt[i];
                            var tableMap = TableMapping.Get(property.DeclaringType);
                            var column = tableMap.GetColumn(property);
                            tracer.TraceInfo("Encrypting all data in {0}.{1}...", tableMap.TableName, column.Name);
                            var statusText = String.Format(UserMessages.ENCRYPTING, $"{tableMap.TableName}.{column.Name}");
                            progressMonitor?.Invoke("ALE_CRYPT", propertiesComplete, statusText);

                            var recordCollectorStmt = context.CreateSqlStatementBuilder($"SELECT * FROM {tableMap.TableName}");
                            // When we update this field it *should* encrypt the database
                            int nRecords = context.Count(recordCollectorStmt.Statement),
                                processed = 0;
                            using (var c2 = context.OpenClonedContext())
                            {
                                foreach (var rec in context.Query(tableMap.OrmType, recordCollectorStmt.Statement))
                                {
                                    c2.Update(rec); // Update should iniitlaize the encryption 
                                    processed++;
                                    progressMonitor?.Invoke("ALE_CRYPT", propertiesComplete + ((float)processed / (float)nRecords) * percentCompletePerProperty, statusText);
                                }
                            }

                            propertiesComplete = (float)i / (float)propertiesToEncrypt.Length;

                        }

                        tx.Commit();
                    }

                }
                catch (Exception e)
                {
                    throw new DataPersistenceException(ErrorMessages.CRYPTO_OPERATION_FAILED, e);
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
                        if (migration.Required)
                        {
                            throw new ConstraintException($"Pre-check for required {migration.Id} failed");
                        }
                        else
                        {
                            return true; // skip
                        }
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

        /// <summary>
        /// Create a backup asset for a provider
        /// </summary>
        public static IBackupAsset CreateBackupAsset(this IDbProvider me, Guid assetId)
        {
            if (me is IDbBackupProvider dbb)
            {
                var tfs = new TemporaryFileStream();
                dbb.BackupToStream(tfs);
                tfs.Seek(0, SeekOrigin.Begin);
                return new StreamBackupAsset(assetId, $"{dbb.GetDatabaseName()}#{dbb.Invariant}", () => tfs);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Restore a backup asset
        /// </summary>
        public static bool RestoreBackupAsset(this IDbProvider me, IBackupAsset backupAsset)
        {
            if (backupAsset == null)
            {
                throw new ArgumentNullException(nameof(backupAsset));
            }

            var assetFname = backupAsset.Name.Split('#');
            if (assetFname.Length != 2 || !me.Invariant.Equals(assetFname[1]))
            {
                throw new InvalidOperationException();
            }
            if (me is IDbBackupProvider dbb)
            {
                using (var str = backupAsset.Open())
                {
                    return dbb.RestoreFromStream(str);
                }
            }
            else
            {
                throw new InvalidOperationException();
            }
        }
    }
}