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
            { "Connection", new string[]{ "server","port","user id","password","database"} },
            { "Pooling", new string[] { "pooling", "minpoolsize", "maxpoolsize" } }
        };

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

    }
}
