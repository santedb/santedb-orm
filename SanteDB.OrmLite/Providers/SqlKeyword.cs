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
 * Date: 2023-6-21
 */
namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// SQL Keywords
    /// </summary>
    public enum SqlKeyword
    {
        /// <summary>
        /// Represents the lower function
        /// </summary>
        Lower,
        /// <summary>
        /// Represents the upper function
        /// </summary>
        Upper,
        /// <summary>
        /// Represents the like function
        /// </summary>
        Like,
        /// <summary>
        /// Represents case insenstivie like
        /// </summary>
        ILike,
        /// <summary>
        /// Represents False (or 0)
        /// </summary>
        False,
        /// <summary>
        /// Represents True (or 1)
        /// </summary>
        True,
        /// <summary>
        /// Create or alter
        /// </summary>
        CreateOrAlter,
        /// <summary>
        /// Refresh materialized view
        /// </summary>
        RefreshMaterializedView,
        /// <summary>
        /// Create a view
        /// </summary>
        CreateView,
        /// <summary>
        /// Create a materialized view
        /// </summary>
        CreateMaterializedView,
        /// <summary>
        /// Intersect 
        /// </summary>
        Intersect,
        /// <summary>
        /// Union distinct
        /// </summary>
        Union,
        /// <summary>
        /// UnionAll
        /// </summary>
        UnionAll,
        /// <summary>
        /// VACUUM FULL
        /// </summary>
        Vacuum,
        /// <summary>
        /// Reindex
        /// </summary>
        Reindex,
        /// <summary>
        /// Analyze
        /// </summary>
        Analyze,
        /// <summary>
        /// Current Timestamp
        /// </summary>
        CurrentTimestamp,
        /// <summary>
        /// Generate a new guid
        /// </summary>
        NewGuid,
        /// <summary>
        /// Defer constraints
        /// </summary>
        DeferConstraints
    }
}