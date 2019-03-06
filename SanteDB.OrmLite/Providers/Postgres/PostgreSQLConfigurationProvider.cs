﻿/*
 * Copyright 2015-2019 Mohawk College of Applied Arts and Technology
 * Copyright 2019-2019 SanteSuite Contributors (See NOTICE)
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
 * User: justi
 * Date: 2019-3-1
 */
using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// PostgreSQL data provider for the configuration system
    /// </summary>
    public class PostgreSQLConfigurationProvider : AdoNetConfigurationProvider
    {

        /// <summary>
        /// Gets the invariant
        /// </summary>
        public override string Name => "ADO.NET PostgreSQL 9 & 10";

        /// <summary>
        /// Invariant name
        /// </summary>
        public override string Invariant => "npgsql";

        /// <summary>
        /// Get the platform that this supports
        /// </summary>
        public override OperatingSystemID Platform => OperatingSystemID.Android | OperatingSystemID.Linux | OperatingSystemID.MacOS | OperatingSystemID.Win32 | OperatingSystemID.Other;

        /// <summary>
        /// Get the options for connecting
        /// </summary>
        public override Dictionary<string, ConfigurationOptionType> Options => new Dictionary<string, ConfigurationOptionType>(){
            { "host", ConfigurationOptionType.String },
            { "port", ConfigurationOptionType.Numeric },
            { "user id", ConfigurationOptionType.String },
            { "password", ConfigurationOptionType.Password },
            { "database", ConfigurationOptionType.DatabaseName },
            { "pooling", ConfigurationOptionType.Boolean },
            { "minpoolsize", ConfigurationOptionType.Numeric },
            { "maxpoolsize", ConfigurationOptionType.Numeric },
        };

        /// <summary>
        /// Option groups
        /// </summary>
        public override Dictionary<String, String[]> OptionGroups => new Dictionary<string, string[]>()
        {
            { "Connection", new string[]{ "host","port","user id","password","database"} },
            { "Pooling", new string[] { "pooling", "minpoolsize", "maxpoolsize" } }
        };

        /// <summary>
        /// Get the database provider
        /// </summary>
        public override Type DbProviderType => typeof(PostgreSQLProvider);

        /// <summary>
        /// Test the connection string
        /// </summary>
        public override bool TestConnectionString(ConnectionString connectionString)
        {
            if (!String.IsNullOrEmpty(connectionString.GetComponent("host")) &&
                !String.IsNullOrEmpty(connectionString.GetComponent("password")) &&
                !String.IsNullOrEmpty(connectionString.GetComponent("database")) &&
                !String.IsNullOrEmpty(connectionString.GetComponent("user id")))
                return base.TestConnectionString(connectionString);
            return false;
        }

        /// <summary>
        /// Create connection string
        /// </summary>
        public override ConnectionString CreateConnectionString(Dictionary<string, object> options)
        {
            if(!options.ContainsKey("port") || String.IsNullOrEmpty(options["port"].ToString()))
            {
                options.Remove("port");
                options.Add("port", 5432);
            }
            return base.CreateConnectionString(options);
        }

        /// <summary>
        /// Get databases
        /// </summary>
        public override IEnumerable<string> GetDatabases(ConnectionString connectionString)
        {
            using (var conn = this.GetProvider(connectionString).GetReadonlyConnection())
            {
                try
                {
                    conn.Open();
                    using (var cmd = conn.Connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT datname FROM pg_database;";
                        List<String> retVal = new List<string>(10);
                        using (var reader = cmd.ExecuteReader())
                            while (reader.Read())
                                retVal.Add(Convert.ToString(reader[0]));
                        return retVal.ToArray();
                    }
                }
                catch
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Get the specified database provider
        /// </summary>
        protected override IDbProvider GetProvider(ConnectionString connectionString) => new PostgreSQLProvider()
        {
            ConnectionString = connectionString.Value
        };

        /// <summary>
        /// Create the specified database
        /// </summary>
        public override ConnectionString CreateDatabase(ConnectionString connectionString, string databaseName, string databaseOwner)
        {
            connectionString = connectionString.Clone();
            connectionString.SetComponent("database", "postgres");
            var provider = this.GetProvider(connectionString);
            using(var conn = provider.GetWriteConnection())
            {
                try
                {
                    // Create the database
                    conn.Open();

                    String[] cmds =
                    {
                        $"CREATE DATABASE {databaseName} WITH OWNER {databaseOwner};",
                        $"REVOKE ALL ON DATABASE {databaseName} FROM public;",
                        $"GRANT ALL ON DATABASE {databaseName} TO {databaseOwner};",
                        $"CREATE OR REPLACE LANGUAGE plpgsql;"
                    };

                    foreach (var cmd in cmds) {
                        using (var c = conn.Connection.CreateCommand())
                        {
                            c.CommandText = cmd;
                            c.CommandType = System.Data.CommandType.Text;
                            c.ExecuteNonQuery();
                        }
                    }

                    connectionString.SetComponent("database", databaseName);
                }
                catch(Exception e)
                {
                    throw new InvalidOperationException($"Could not create database: {e.Message}", e);
                }
            }

            return connectionString;


        }
    }
}
