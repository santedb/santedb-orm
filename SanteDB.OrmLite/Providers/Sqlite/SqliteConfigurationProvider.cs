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
 * Date: 2023-5-19
 */
using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Diagnostics;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// SQLite Configuration Provider
    /// </summary>
    public class SqliteConfigurationProvider : AdoNetConfigurationProvider
    {

        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(SqliteConfigurationProvider));

        /// <summary>
        /// Gets the invariant name of the configuration provider
        /// </summary>
        public override string Invariant => SqliteProvider.InvariantName;

        /// <summary>
        /// Get the name of the SQL provdier
        /// </summary>
        public override string Name => "ADO.NET Sqlite / SqlCipher";

        /// <summary>
        /// Get the platforms on which this operates
        /// </summary>
        public override OperatingSystemID Platform => OperatingSystemID.Win32 | OperatingSystemID.Android | OperatingSystemID.iOS | OperatingSystemID.Linux | OperatingSystemID.MacOS | OperatingSystemID.Other;

        /// <summary>
        /// Get the provider factory
        /// </summary>
        public override Type AdoNetFactoryType => Type.GetType(SqliteProvider.ProviderFactoryType);

        /// <summary>
        /// Get the host types that this works on
        /// </summary>
        public override SanteDBHostType HostType => SanteDBHostType.Client | SanteDBHostType.Configuration | SanteDBHostType.Gateway | SanteDBHostType.Other | SanteDBHostType.Server | SanteDBHostType.Test;

        /// <summary>
        /// Gets the options for configuring this provider
        /// </summary>
        public override IDictionary<string, ConfigurationOptionType> Options => new Dictionary<String, ConfigurationOptionType>()
        {
            { "Data Source", ConfigurationOptionType.DatabaseName },
            { "Password", ConfigurationOptionType.Password },
            { "Foreign Keys", ConfigurationOptionType.Boolean }
        };

        /// <summary>
        /// Option groups
        /// </summary>
        public override IDictionary<string, string[]> OptionGroups => new Dictionary<String, String[]>()
                {
                    { "Connection", this.Options.Keys.ToArray() }
                };

        /// <summary>
        /// Get the database provider type
        /// </summary>
        public override Type DbProviderType => typeof(SqliteProvider);

        /// <inheritdoc/>
        public override void DropDatabase(ConnectionString connectionString, string databaseName)
        {

            connectionString = connectionString.Clone();

            if (String.IsNullOrEmpty(Path.GetExtension(databaseName)))
            {
                databaseName = Path.ChangeExtension(databaseName, "sqlite");
            }
            connectionString.SetComponent("Data Source", databaseName);
            connectionString = SqliteProvider.CorrectConnectionString(connectionString);
            databaseName = connectionString.GetComponent("Data Source");

            try
            {
                //HACK: Clear out the connection pool associated with the connection
                var poolClear = Type.GetType("Microsoft.Data.Sqlite.SqliteConnection, Microsoft.Data.Sqlite");
                if (poolClear != null)
                {
                    poolClear.GetMethod("ClearAllPools").Invoke(null, new object[0]);
                }

                if (File.Exists(databaseName))
                {
                    File.Delete(databaseName);
                }
                else if (!Path.IsPathRooted(databaseName))
                {
                    var newPath = Path.Combine(Path.GetDirectoryName(typeof(SqliteConfigurationProvider).Assembly.Location), databaseName);
                    if (File.Exists(newPath))
                    {
                        File.Delete(newPath);
                    }
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Could not drop database: {e.Message}", e);
            }

        }

        /// <summary>
        /// Create a new database 
        /// </summary>
        public override ConnectionString CreateDatabase(ConnectionString connectionString, string databaseName, string databaseOwner)
        {
            connectionString = connectionString.Clone();

            if (String.IsNullOrEmpty(Path.GetExtension(databaseName)))
            {
                databaseName = Path.ChangeExtension(databaseName, "sqlite");
            }
            connectionString.SetComponent("Data Source", databaseName);
            var provider = this.GetProvider(connectionString);

            using (var conn = provider.GetWriteConnection())
            {
                try
                {
                    // Create the database
                    conn.Open();

                    var newConnectionString = SqliteProvider.CorrectConnectionString(connectionString);
                    var password = newConnectionString.GetComponent("Password");
                    if (!String.IsNullOrEmpty(password))
                    {

                        using (var c = conn.Connection.CreateCommand())
                        {
                            c.CommandText = "SELECT quote($password)";
                            var passwordParm = c.CreateParameter();
                            passwordParm.ParameterName = "$password";
                            passwordParm.Value = password;
                            c.Parameters.Add(passwordParm);
                            c.CommandText = $"PRAGMA rekey = {c.ExecuteScalar()}";
                            c.Parameters.Clear();
                            c.ExecuteNonQuery();
                        }
                    }

                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Could not create database: {e.Message}", e);
                }
            }

            return connectionString;
        }

        /// <summary>
        /// Get all databases
        /// </summary>
        public override IEnumerable<string> GetDatabases(ConnectionString connectionString)
        {
            var dbPath = connectionString.GetComponent("Data Source");
            if (String.IsNullOrEmpty(dbPath))
            {
                return Directory.GetFiles(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "*.sqlite").Select(o => Path.GetFileName(o));
            }
            else
            {
                var dataDirectory = AppDomain.CurrentDomain.GetData("DataDirectory")?.ToString();
                dbPath = dbPath.Replace("|DataDirectory|", dataDirectory);
                if (Path.IsPathRooted(dbPath))
                {
                    return Directory.GetFiles(Path.GetDirectoryName(dbPath), "*.sqlite").Select(o => Path.GetFileName(o));
                }
                else if (!String.IsNullOrEmpty(dataDirectory))
                {
                    return Directory.GetFiles(dataDirectory, "*.sqlite").Select(o => Path.GetFileName(o));
                }
                else
                {
                    return Directory.GetFiles(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "*.sqlite").Select(o => Path.GetFileName(o));
                }
            }
        }

        /// <inheritdoc/>
        protected override IDbProvider GetProvider(ConnectionString connectionString) => new SqliteProvider()
        {
            ConnectionString = SqliteProvider.CorrectConnectionString(connectionString).Value
        };

        /// <inheritdoc/>
        public override DataConfigurationCapabilities Capabilities => new DataConfigurationCapabilities("Data Source", null, "Password", null, false);

        /// <inheritdoc/>
        public override ConnectionString CreateConnectionString(IDictionary<string, object> options)
        {
            if (options.TryGetValue("Data Source", out var dataSourceRaw) && Path.GetExtension(dataSourceRaw.ToString()) != "sqlite")
            {
                dataSourceRaw = options["Data Source"] = Path.ChangeExtension(dataSourceRaw.ToString(), "sqlite");
            }

            if (!options.ContainsKey("Foreign Keys"))
            {
                options.Add("Foreign Keys", false);
            }

            return base.CreateConnectionString(options);
        }
    }
}
