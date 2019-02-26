﻿using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Migration;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents a base ADO.NET data provider
    /// </summary>
    public abstract class AdoNetConfigurationProvider : IDataConfigurationProvider
    {
        /// <summary>
        /// Gets the provider
        /// </summary>
        protected abstract IDbProvider GetProvider(ConnectionString connectionString);

        /// <summary>
        /// Get the invariant
        /// </summary>
        public abstract string Invariant { get; }

        /// <summary>
        /// Gets the name
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// Gets the platform
        /// </summary>
        public abstract OperatingSystemID Platform { get; }
        /// <summary>
        /// Gets the host type
        /// </summary>
        public SanteDBHostType HostType => SanteDBHostType.Server;

        /// <summary>
        /// Gets the options
        /// </summary>
        public abstract Dictionary<string, ConfigurationOptionType> Options { get; }

        /// <summary>
        /// Get the option groups
        /// </summary>
        public virtual Dictionary<String, String[]> OptionGroups {
            get
            {
                return new Dictionary<string, string[]>() { { "Connection", this.Options.Keys.ToArray() } };
            }
        }

        /// <summary>
        /// Fired when progress is being made
        /// </summary>
        public event EventHandler<ProgressChangedEventArgs> ProgressChanged;

        /// <summary>
        /// Configure the specified provider
        /// </summary>
        public bool Configure(SanteDBConfiguration configuration, Dictionary<string, object> options)
        {
            return true;
        }

        /// <summary>
        /// Deploy the specified feature
        /// </summary>
        public virtual bool Deploy(IDataFeature feature, string connectionStringName, SanteDBConfiguration configuration)
        {
            return false;
        }

        /// <summary>
        /// Get databases
        /// </summary>
        public abstract IEnumerable<string> GetDatabases(ConnectionString connectionString);

        /// <summary>
        /// Get applicable data features that can be deployed
        /// </summary>
        public IEnumerable<IDataFeature> GetFeatures(ConnectionString connectionString)
        {
            // Get all embedded data features
            var provider = this.GetProvider(connectionString);

            using(var conn = provider.GetReadonlyConnection())
                return SqlFeatureUtil.GetFeatures(this.Invariant).Where(f =>
                {
                    try
                    {
                        var checkSql = f.GetCheckSql(this.Invariant);
                        if (!String.IsNullOrEmpty(checkSql)) 
                            using (var cmd = conn.Connection.CreateCommand())
                            {
                                cmd.CommandText = checkSql;
                                cmd.CommandType = System.Data.CommandType.Text;
                                return (bool)cmd.ExecuteScalar();
                            }
                        return false;
                    }
                    catch (Exception e){
                        Trace.TraceError("Error executing pre-check {0}: {1}", f.Name, e);
                        return false;
                    }
                }).ToArray();
        }


        /// <summary>
        /// Create a connection string
        /// </summary>
        public virtual ConnectionString CreateConnectionString(Dictionary<string, object> options)
        {
            return new ConnectionString(this.Invariant, options);
        }

        /// <summary>
        /// Parse connection string
        /// </summary>
        public virtual Dictionary<string, object> ParseConnectionString(ConnectionString connectionString)
        {
            var retVal = this.Options.Keys.ToDictionary(o=>o, p=>(Object)null);
            foreach (var itm in retVal)
                retVal[itm.Key] = connectionString.GetComponent(itm.Key);
            return retVal;
        }
    }
}
