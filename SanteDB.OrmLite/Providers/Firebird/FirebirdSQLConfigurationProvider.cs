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
using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Configuration.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace SanteDB.OrmLite.Providers.Firebird
{
    /// <summary>
    /// Represents a FirebirdSQLDataProvider
    /// </summary>
    public class FirebirdSQLConfigurationProvider : AdoNetConfigurationProvider
    {
        /// <summary>
        /// Get the invariant name
        /// </summary>
        public override string Invariant => FirebirdSQLProvider.InvariantName;

        /// <summary>
        /// Get the name
        /// </summary>
        public override string Name => "ADO.NET FirebirdSQL 3.x";

        /// <summary>
        /// Get the available platforms
        /// </summary>
        public override OperatingSystemID Platform => OperatingSystemID.Win32;

        /// <summary>
        /// Get the database provider
        /// </summary>
        public override Type DbProviderType => typeof(FirebirdSQLProvider);

        /// <summary>
        /// Get the provider factory
        /// </summary>
        public override Type AdoNetFactoryType => Type.GetType(FirebirdSQLProvider.ProviderFactoryType);

        /// <summary>
        /// Get the options
        /// </summary>
        public override IDictionary<string, ConfigurationOptionType> Options => new Dictionary<string, ConfigurationOptionType>()
        {
            { "user id", ConfigurationOptionType.User },
            { "password", ConfigurationOptionType.Password },
            { "initial catalog", ConfigurationOptionType.DatabaseName }
        };

        /// <summary>
        /// Create a connection string from the specified options
        /// </summary>
        public override ConnectionString CreateConnectionString(IDictionary<string, object> options)
        {
            return this.CorrectConnectionString(new ConnectionString(this.Invariant, options));
        }

        /// <summary>
        /// Test the connection string
        /// </summary>
        public override bool TestConnectionString(ConnectionString connectionString)
        {
            if (!String.IsNullOrEmpty(connectionString.GetComponent("initial catalog")) &&
                !String.IsNullOrEmpty(connectionString.GetComponent("user id")))
            {
                return base.TestConnectionString(connectionString);
            }

            return false;
        }

        /// <summary>
        /// Correct the specified connection string
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        private ConnectionString CorrectConnectionString(ConnectionString connectionString)
        {
            if (String.IsNullOrEmpty(connectionString.GetComponent("server type")))
            {
                connectionString.SetComponent("server type", "Embedded");
            }

            if (!String.IsNullOrEmpty(connectionString.GetComponent("initial catalog"))
                && !connectionString.GetComponent("initial catalog").StartsWith("|DataDirectory|")
                && !Path.IsPathRooted(connectionString.GetComponent("initial catalog")))
            {
                connectionString.SetComponent("initial catalog", $"|DataDirectory|{Path.DirectorySeparatorChar}{connectionString.GetComponent("initial catalog")}");
            }
            else
            {
                connectionString.SetComponent("initial catalog", connectionString.GetComponent("initial catalog").Replace("\\", Path.DirectorySeparatorChar.ToString()));
            }

            connectionString.SetComponent("Charset", "NONE");

            //var clientLibraryName = "fbclient.dll";
            //if (Environment.OSVersion.Platform == PlatformID.Unix)
            //{
            //    clientLibraryName = "libfbclient.so";
            //}
            //connectionString.SetComponent("client library", clientLibraryName);
            return connectionString;
        }

        /// <summary>
        /// Create the specified database
        /// </summary>
        public override ConnectionString CreateDatabase(ConnectionString connectionString, string databaseName, string databaseOwner)
        {
            return this.InvokeClientMethod("CreateDatabase", connectionString, databaseName);
        }

        /// <inheritdoc/>
        public override void DropDatabase(ConnectionString connectionString, string databaseName)
        {
            this.InvokeClientMethod("DropDatabase", connectionString, databaseName);
        }

        /// <summary>
        /// Invoke a client method
        /// </summary>
        private ConnectionString InvokeClientMethod(string methodName, ConnectionString connectionString, string databaseName)
        {
            // This is a little tricky as we have to get the FireBird ADO and call the function through reflection since ORM doesn't have it
            connectionString = connectionString.Clone();
            connectionString.SetComponent("server type", "Embedded");

            var clientLibraryName = "fbclient.dll";
            if (Environment.OSVersion.Platform == PlatformID.Unix)
            {
                clientLibraryName = "libfbclient.so";
            }
            connectionString.SetComponent("client library", clientLibraryName);

            var fbConnectionType = Type.GetType("FirebirdSql.Data.FirebirdClient.FbConnection, FirebirdSql.Data.FirebirdClient");
            if (fbConnectionType == null)
            {
                throw new InvalidOperationException($"Cannot find FirebirdSQL provider library, ensure that: \r\n\t - fbclient.dll is present and is compiled for {(Environment.Is64BitProcess ? "x64" : "x86")}\r\n\t - The Firebird provider has been installed in this SanteDB Server");
            }

            var createDbMethod = fbConnectionType.GetMethods(System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public).SingleOrDefault(o => o.Name == methodName && o.GetParameters().Length == 4);
            if (createDbMethod == null)
            {
                throw new InvalidOperationException($"Cannot find FirebirdSQL {methodName} method. Perhaps this is an invalid version of ADO.NET provider");
            }

            var dbPath = Path.ChangeExtension(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), databaseName), "fdb");
            dbPath = dbPath.Replace("|DataDirectory|", AppDomain.CurrentDomain.GetData("DataDirectory").ToString()).Replace("\\", Path.DirectorySeparatorChar.ToString());
            connectionString.SetComponent("initial catalog", dbPath);
            createDbMethod.Invoke(null, new object[] { connectionString.ToString(), 4096, true, false });
            connectionString.SetComponent("initial catalog", Path.GetFileName(dbPath));
            return connectionString;
        }

        /// <summary>
        /// Get databases
        /// </summary>
        public override IEnumerable<string> GetDatabases(ConnectionString connectionString)
        {
            var dbPath = connectionString.GetComponent("initial catalog");
            if (String.IsNullOrEmpty(dbPath))
            {
                return Directory.GetFiles(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "*.fdb").Select(o => Path.GetFileName(o));
            }
            else
            {
                dbPath = dbPath.Replace("|DataDirectory|", AppDomain.CurrentDomain.GetData("DataDirectory").ToString()).Replace("\\", Path.DirectorySeparatorChar.ToString());
                if (Path.IsPathRooted(dbPath))
                {
                    return Directory.GetFiles(Path.GetDirectoryName(dbPath), "*.fdb").Select(o => Path.GetFileName(o));
                }
                else
                {
                    return Directory.GetFiles(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "*.fdb").Select(o => Path.GetFileName(o));
                }
            }
        }

        /// <summary>
        /// Parse the specified connection string into a series of objects
        /// </summary>
        public override IDictionary<string, object> ParseConnectionString(ConnectionString connectionString)
        {
            return new Dictionary<string, object>()
            {
                { "user id", connectionString.GetComponent("user id") },
                { "password", connectionString.GetComponent("password") },
                { "initial catalog", connectionString.GetComponent("initial catalog") },
                { "database", "RDB$DATABASE" }
            };
        }

        /// <summary>
        /// Get the specified database provider
        /// </summary>
        protected override IDbProvider GetProvider(ConnectionString connectionString) => new SanteDB.OrmLite.Providers.Firebird.FirebirdSQLProvider()
        {
            ConnectionString = this.CorrectConnectionString(connectionString).Value
        };

        /// <inheritdoc/>
        public override DataConfigurationCapabilities Capabilities => new DataConfigurationCapabilities("initial catalog", "user id", "password", null, true);

    }
}