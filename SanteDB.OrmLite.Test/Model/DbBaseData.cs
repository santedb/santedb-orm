﻿/*
 * Copyright (C) 2021 - 2022, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2022-5-30
 */
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model
{
    /// <summary>
    /// Base data interface
    /// </summary>
    public interface IDbBaseData : IDbIdentified
    {
        /// <summary>
        /// Gets or sets the entity id which created this
        /// </summary>
        Guid CreatedByKey { get; set; }
        /// <summary>
        /// Gets or sets the id which obsoleted this
        /// </summary>
        Guid? ObsoletedByKey { get; set; }
        /// <summary>
        /// Gets or sets the creation time
        /// </summary>
        DateTimeOffset CreationTime { get; set; }
        /// <summary>
        /// Gets or sets the obsoletion time
        /// </summary>
        DateTimeOffset? ObsoletionTime { get; set; }

        /// <summary>
        /// Gets or sets whether the obsoleted by is specified (specifically null)
        /// </summary>
        bool ObsoletedByKeySpecified { get; set; }
        /// <summary>
        /// Gets or sets whether to obsoletion time is specified (specifically null)
        /// </summary>
        bool ObsoletionTimeSpecified { get; set; }
    }

    /// <summary>
    /// Represents non-versioendd base data
    /// </summary>
    public interface IDbNonVersionedBaseData : IDbBaseData
    {
        /// <summary>
        /// Gets or sets the updated user
        /// </summary>
        Guid? UpdatedByKey { get; set; }

        /// <summary>
        /// Gets or sets the time of updating
        /// </summary>
        DateTimeOffset? UpdatedTime { get; set; }
    }

    /// <summary>
    /// Represents base data
    /// </summary>
    [ExcludeFromCodeCoverage]
    public abstract class DbBaseData : DbIdentified, IDbBaseData
    {
        /// <summary>
        /// Gets or sets the entity id which created this
        /// </summary>
        [Column("crt_prov_id"), ForeignKey(typeof(DbSecurityProvenance), nameof(DbSecurityProvenance.Key))]
        public Guid CreatedByKey { get; set; }
        /// <summary>
        /// Gets or sets the id which obsoleted this
        /// </summary>
        [Column("obslt_prov_id"), ForeignKey(typeof(DbSecurityProvenance), nameof(DbSecurityProvenance.Key))]
        public Guid? ObsoletedByKey { get; set; }
        /// <summary>
        /// Gets or sets the creation time
        /// </summary>
        [Column("crt_utc"), AutoGenerated]
        public DateTimeOffset CreationTime { get; set; }
        /// <summary>
        /// Gets or sets the obsoletion time
        /// </summary>
        [Column("obslt_utc")]
        public DateTimeOffset? ObsoletionTime { get; set; }

        /// <summary>
        /// Identifies whether obsoletion time is specified
        /// </summary>
        public bool ObsoletionTimeSpecified { get; set; }
        /// <summary>
        /// Identifies whether obsoletion time is specified
        /// </summary>
        public bool ObsoletedByKeySpecified { get; set; }
    }

    /// <summary>
    /// Non-versioned base data
    /// </summary>
    [ExcludeFromCodeCoverage]
    public abstract class DbNonVersionedBaseData : DbBaseData, IDbNonVersionedBaseData
    {

        /// <summary>
        /// Gets or sets the updated user
        /// </summary>
        [Column("upd_prov_id"), ForeignKey(typeof(DbSecurityProvenance), nameof(DbSecurityProvenance.Key))]
        public Guid? UpdatedByKey { get; set; }

        /// <summary>
        /// Gets or sets the time of updating
        /// </summary>
        [Column("upd_utc")]
        public DateTimeOffset? UpdatedTime { get; set; }
    }
    
}
