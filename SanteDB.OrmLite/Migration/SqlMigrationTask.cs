/*
 * Based on OpenIZ, Copyright (C) 2015 - 2019 Mohawk College of Applied Arts and Technology
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
using SanteDB.Core.Configuration;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Configuration;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite.Migration
{
    /// <summary>
    /// Represents a single installation task for a SQL feature
    /// </summary>
    public class SqlMigrationTask : IDescribedConfigurationTask
    {

        /// <summary>
        /// Get the name of the task
        /// </summary>
        public String Name => this.m_feature.Name;

        /// <summary>
        /// Get the description
        /// </summary>
        public String Description => this.m_feature.Description;

        /// <summary>
        /// Gets additional information
        /// </summary>
        public String AdditionalInformation => this.m_feature.Remarks;

        /// <summary>
        /// Gets the help URL
        /// </summary>
        public Uri HelpUri => this.m_feature.Url;

        // SQL Feature
        private SqlFeature m_feature;

        /// <summary>
        /// Creates a SqlMigrationTask
        /// </summary>
        public SqlMigrationTask(IFeature feature, SqlFeature dbFeature)
        {
            this.Feature = feature;
            this.m_feature = dbFeature;
        }

        /// <summary>
        /// Gets the feature which owns the task
        /// </summary>
        public IFeature Feature { get; }

        /// <summary>
        /// Progress has changed
        /// </summary>
        public event EventHandler<ProgressChangedEventArgs> ProgressChanged;

        /// <summary>
        /// Execute the configuration
        /// </summary>
        public bool Execute(SanteDBConfiguration configuration)
        {
            try
            {
                if (!this.VerifyState(configuration))
                    return true;

                var config = this.Feature.Configuration as OrmConfigurationBase;
                using (var conn = config.Provider.GetWriteConnection())
                {
                    conn.Open();

                    string deploySql = this.m_feature.GetDeploySql();

                    // Prepare SQL statements

                    // Check SQL 
                    if (!String.IsNullOrEmpty(deploySql))
                    {
                        var stmts = deploySql.Split(new string[] { "--#!" }, StringSplitOptions.RemoveEmptyEntries);
                        int i = 0;
                        foreach (var dsql in stmts)
                            using (var cmd = conn.Connection.CreateCommand())
                            {
                                if (String.IsNullOrEmpty(dsql.Trim())) continue;
                                this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs(((float)i++ / stmts.Length), $"Deploying {this.m_feature.Name}..."));
                                cmd.CommandText = dsql;
                                cmd.CommandType = System.Data.CommandType.Text;
                                Debug.WriteLine("Execute: {0}", dsql);
                                cmd.ExecuteNonQuery();
                            }
                    }
                    return true;
                }
            }
            catch (Exception e)
            {
                throw new DataException($"Error deploying {this.m_feature.Name} : {e.Message}", e);
            }
        }

        /// <summary>
        /// Verify the configuration
        /// </summary>
        public bool VerifyState(SanteDBConfiguration configuration)
        {
            try
            {
                var config = this.Feature.Configuration as OrmConfigurationBase;
                using (var conn = config.Provider.GetWriteConnection())
                {
                    conn.Open();

                    string checkSql = this.m_feature.GetCheckSql(),
                        preConditionSql = this.m_feature.GetPreCheckSql();


                    if(!String.IsNullOrEmpty(preConditionSql))
                        using (var cmd = conn.Connection.CreateCommand())
                        {
                            cmd.CommandText = preConditionSql;
                            cmd.CommandType = System.Data.CommandType.Text;
                            if (!(bool)cmd.ExecuteScalar()) // can't install
                                throw new ConstraintException($"Pre-check for {this.Name} failed");
                            
                        }
                    if (!String.IsNullOrEmpty(checkSql))
                        using (var cmd = conn.Connection.CreateCommand())
                        {
                            cmd.CommandText = checkSql;
                            cmd.CommandType = System.Data.CommandType.Text;
                            return !(bool)cmd.ExecuteScalar();
                        }

                    return true;
                }
            }
            catch (System.Data.Common.DbException) // CHECK FAILED & MUST SUCCEED IS FALSE = INSTALL
            {
                return !this.m_feature.MustSucceed;
            }
            catch (Exception e)
            {
                throw new DataException($"Error deploying {this.m_feature.Name} : {e.Message}", e);
            }
        }

        /// <summary>
        /// Rollback is not supported
        /// </summary>
        public bool Rollback(SanteDBConfiguration configuration)
        {
            return false;
        }
    }
}
