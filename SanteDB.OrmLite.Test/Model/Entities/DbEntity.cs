﻿/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-3-10
 */
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using SanteDB.Persistence.Data.ADO.Data.Model.Extensibility;
using System;
using System.Diagnostics.CodeAnalysis;


namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// Represents an entity in the database
    /// </summary>
    [ExcludeFromCodeCoverage]
    [Table("ent_tbl")]
    public class DbEntity : DbIdentified
    {
        /// <summary>
        /// Gets or sets the template
        /// </summary>
        [Column("tpl_id"), ForeignKey(typeof(DbTemplateDefinition), nameof(DbTemplateDefinition.Key))]
        public Guid TemplateKey { get; set; }

        /// <summary>
        /// Gets or sets the class concept identifier.
        /// </summary>
        /// <value>The class concept identifier.</value>
        [Column("cls_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid ClassConceptKey
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the determiner concept identifier.
        /// </summary>
        /// <value>The determiner concept identifier.</value>
        [Column("dtr_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid DeterminerConceptKey
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the key
        /// </summary>
        [Column("ent_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }
    }
}

