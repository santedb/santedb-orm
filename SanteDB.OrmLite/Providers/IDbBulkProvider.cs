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
 */
using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents a bulk data provider which can read/write from memory for faster inserts
    /// </summary>
    public interface IDbBulkProvider : IDbProvider
    {

        /// <summary>
        /// Gets a memory connection or temporary connection where data can be inserted quickly
        /// </summary>
        /// <returns>The memory data connection</returns>
        DataContext GetBulkConnection();

        /// <summary>
        /// Flushes the contents of <paramref name="bulkContext"/> into a connection obtained via <see cref="IDbProvider.GetWriteConnection"/>
        /// </summary>
        /// <param name="bulkContext">The data context opened by <see cref="GetBulkConnection"/> which is to be flushed</param>
        void FlushBulkConnection(DataContext bulkContext);

    }
}
