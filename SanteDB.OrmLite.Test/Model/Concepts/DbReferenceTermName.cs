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
using SanteDB.Persistence.Data.ADO.Data.Model.DataType;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Concepts
{
    /// <summary>
    /// Reference term name
    /// </summary>
    [ExcludeFromCodeCoverage]
    [Table("ref_term_name_tbl")]
    public class DbReferenceTermName : DbAssociation, IDbBaseData
    {
        /// <summary>
        /// Gets or sets the key
        /// </summary>
        [Column("ref_term_name_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the ref term to which the nae applies
        /// </summary>
        [Column("ref_term_id"), ForeignKey(typeof(DbReferenceTerm), nameof(DbReferenceTerm.Key))]
        public override Guid SourceKey { get; set; }

        /// <summary>
        /// Created by 
        /// </summary>
        [Column("crt_prov_id"), ForeignKey(typeof(DbSecurityProvenance), nameof(DbSecurityProvenance.Key))]
        public Guid CreatedByKey { get; set; }

        /// <summary>
        /// Creation time
        /// </summary>
        [Column("crt_utc"), AutoGenerated]
        public DateTimeOffset CreationTime { get; set; }

        /// <summary>
        /// Obsoleted by 
        /// </summary>
        [Column("obslt_prov_id"), ForeignKey(typeof(DbSecurityProvenance), nameof(DbSecurityProvenance.Key))]
        public Guid? ObsoletedByKey { get; set; }

        /// <summary>
        /// Gets or sets the obsoletion time
        /// </summary>
        [Column("obslt_utc")]
        public DateTimeOffset? ObsoletionTime { get; set; }


        /// <summary>
        /// Gets or sets the language code
        /// </summary>
        [Column("lang_cs")]
        public String LanguageCode { get; set; }

        /// <summary>
        /// Gets orsets the value
        /// </summary>
        [Column("term_name")]
        public String Value { get; set; }

        /// <summary>
        /// Gets or sets the phonetic code
        /// </summary>
        [Column("phon_cs")]
        public String PhoneticCode { get; set; }

        /// <summary>
        /// Gets or sets the algorithm id
        /// </summary>
        [Column("phon_alg_id"), ForeignKey(typeof(DbPhoneticAlgorithm), nameof(DbPhoneticAlgorithm.Key))]
        public Guid PhoneticAlgorithm { get; set; }

        /// <summary>
        /// Gets or sets whether obsoleted by key is specified (for undelete)
        /// </summary>
        public bool ObsoletedByKeySpecified { get; set; }

        /// <summary>
        /// Gets or sets whether obsoletion time is specified
        /// </summary>
        public bool ObsoletionTimeSpecified { get; set; }
    }
}
