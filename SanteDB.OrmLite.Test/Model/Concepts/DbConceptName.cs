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
using System;
using System.Diagnostics.CodeAnalysis;


namespace SanteDB.Persistence.Data.ADO.Data.Model.Concepts
{
    /// <summary>
    /// Represents a concept name
    /// </summary>
    [Table("cd_name_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbConceptName : DbConceptVersionedAssociation
    {

        /// <summary>
        /// Gets or sets the language.
        /// </summary>
        /// <value>The language.</value>
        [Column("lang_cs")]
        public String Language
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        /// <value>The name.</value>
        [Column("val")]
        public String Name
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the phonetic code.
        /// </summary>
        /// <value>The phonetic code.</value>
        [Column("phon_cs")]
        public String PhoneticCode
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the phonetic algorithm identifier.
        /// </summary>
        /// <value>The phonetic algorithm identifier.</value>
        [Column("phon_alg_id")]
        public Guid PhoneticAlgorithmKey
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the id of the name
        /// </summary>
        [Column("name_id"), AutoGenerated, PrimaryKey]
        public override Guid Key { get; set; }
    }
}

