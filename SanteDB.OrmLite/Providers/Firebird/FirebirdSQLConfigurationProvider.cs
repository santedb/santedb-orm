using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Configuration.Data;
using SanteDB.OrmLite.Providers;

namespace SanteDB.Configurator.DataProviders
{
    /// <summary>
    /// Represents a FirebirdSQLDataProvider
    /// </summary>
    public class FirebirdSQLConfigurationProvider : AdoNetConfigurationProvider
    {
        /// <summary>
        /// Get the invariant name
        /// </summary>
        public override string Invariant => "fbsql";

        /// <summary>
        /// Get the name
        /// </summary>
        public override string Name => "ADO.NET FirebirdSQL 3.x";

        /// <summary>
        /// Get the available platforms
        /// </summary>
        public override OperatingSystemID Platform => OperatingSystemID.Win32;

        /// <summary>
        /// Get the options
        /// </summary>
        public override Dictionary<string, ConfigurationOptionType> Options => new Dictionary<string, ConfigurationOptionType>()
        {
            { "user id", ConfigurationOptionType.String },
            { "password", ConfigurationOptionType.Password },
            { "initial catalog", ConfigurationOptionType.FileName }
        };

        /// <summary>
        /// Create a connection string from the specified options
        /// </summary>
        public override ConnectionString CreateConnectionString(Dictionary<string, object> options)
        {
            return new ConnectionString(this.Invariant, options);
        }

        /// <summary>
        /// Get databases
        /// </summary>
        public override IEnumerable<string> GetDatabases(ConnectionString connectionString)
        {
            return new String[] { "RDB$DATABASE" };
        }

        /// <summary>
        /// Parse the specified connection string into a series of objects
        /// </summary>
        public override Dictionary<string, object> ParseConnectionString(ConnectionString connectionString)
        {
            return new Dictionary<string, object>()
            {
                { "user id", connectionString.GetComponent("user id") },
                { "password", connectionString.GetComponent("password") },
                { "initial catalog", connectionString.GetComponent("initial catalog") }
            };
        }

        /// <summary>
        /// Get the specified database provider
        /// </summary>
        protected override IDbProvider GetProvider(ConnectionString connectionString) => new SanteDB.OrmLite.Providers.Firebird.FirebirdSQLProvider()
        {
            ConnectionString = connectionString.Value
        };
    }
}
