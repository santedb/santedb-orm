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
    /// A function which can translate codes to indexing 
    /// </summary>
    public interface IDbIndexFunction
    {

        /// <summary>
        /// Gets the name of the index
        /// </summary>
        String Name { get; }

        /// <summary>
        /// Gets the provider to which this index function applies
        /// </summary>
        String Provider { get; }

        /// <summary>
        /// Create the statement to define the index
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="column">The column to be indexed</param>
        /// <param name="tableName">The table to be indexed</param>
        SqlStatementBuilder CreateIndex(String indexName, String tableName, String column);


    }
}
