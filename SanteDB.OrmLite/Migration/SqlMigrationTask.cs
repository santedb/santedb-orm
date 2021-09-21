/*
 * Copyright (C) 2021 - 2021, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-8-5
 */
using SanteDB.Core.Configuration;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Configuration;
using System;
using System.Data;

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
                    // Check SQL 
                    return conn.Install(this.m_feature);
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
                    return !conn.IsInstalled(this.m_feature);
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
