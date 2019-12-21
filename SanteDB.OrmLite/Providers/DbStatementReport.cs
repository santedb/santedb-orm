/*
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
 * User: Justin Fyfe
 * Date: 2019-9-1
 */
using System;

namespace SanteDB.OrmLite.Providers
{

    /// <summary>
    /// States of the database statement
    /// </summary>
    public enum DbStatementStatus
    {
        Idle,
        Active,
        Stalled,
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