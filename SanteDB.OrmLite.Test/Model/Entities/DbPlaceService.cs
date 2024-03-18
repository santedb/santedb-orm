﻿/*
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
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents the orm class for place service
    /// </summary>
    [Table("plc_svc_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbPlaceService : DbEntityVersionedAssociation
    {

        /// <summary>
        /// Gets or sets the service schedule information
        /// </summary>
        [Column("schdl")]
        public String ServiceSchedule { get; set; }

        /// <summary>
        /// Gets or sets the service concept
        /// </summary>
        [Column("svc_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid ServiceConceptKey { get; set; }

        /// <summary>
        /// Primary key
        /// </summary>
        [Column("svc_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }
    }
}
