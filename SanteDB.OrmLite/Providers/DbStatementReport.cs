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
using System;

namespace SanteDB.OrmLite.Providers
{

    /// <summary>
    /// States of the database statements which are returned from <see cref="IDbMonitorProvider.StatActivity"/>
    /// </summary>
    public enum DbStatementStatus
    {
        /// <summary>
        /// The connection/statement is idle
        /// </summary>
        Idle,
        /// <summary>
        /// The connection/statement is active
        /// </summary>
        Active,
        /// <summary>
        /// The statement is stalled
        /// </summary>
        Stalled,
        /// <summary>
        /// Another unknown state
        /// </summary>
        Other
    }

    /// <summary>
    /// Represents a single database status record
    /// </summary>
    public class DbStatementReport
    {

        /// <summary>
        /// Gets the statement identifier
        /// </summary>
        public string StatementId { get; set; }

        /// <summary>
        /// Gets the current state of the action
        /// </summary>
        public DbStatementStatus Status { get; set; }

        /// <summary>
        /// Gets the current query
        /// </summary>
        public string Query { get; set; }

        /// <summary>
        /// Gets the start time
        /// </summary>
        public DateTime Start { get; set; }

    }
}