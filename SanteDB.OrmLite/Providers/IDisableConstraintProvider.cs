/*
 * Copyright (C) 2021 - 2026, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2024-11-21
 */
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents a database provider that can disable or enable constraints progammatically
    /// </summary>
    public interface IDisableConstraintProvider : IDbProvider
    {
        /// <summary>
        /// Disable all constraints on the connection
        /// </summary>
        /// <param name="context">The connection to disable constraint checks</param>
        void DisableAllConstraints(DataContext context);

        /// <summary>
        /// Enable all constraints on the connection
        /// </summary>
        /// <param name="context">The connection to enable constraint checks</param>
        void EnableAllConstraints(DataContext context);

    }
}
