﻿/*
 * Copyright 2015-2019 Mohawk College of Applied Arts and Technology
 *
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
 * User: JustinFyfe
 * Date: 2019-1-22
 */
using SanteDB.Core.Model.Constants;
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Concepts;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Acts
{
    /// <summary>
    /// Represents a procedure in the data model
    /// </summary>
    [Table("proc_tbl")]
    public class DbProcedure : DbActSubTable
    {

        /// <summary>
        /// Parent key
        /// </summary>
        [JoinFilter(PropertyName = nameof(DbAct.ClassConceptKey), Value = ActClassKeyStrings.Procedure)]
        public override Guid ParentKey
        {
            get
            {
                return base.ParentKey;
            }

            set
            {
                base.ParentKey = value;
            }
        }

        /// <summary>
        /// Gets or sets the technique used 
        /// </summary>
        [Column("mth_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid? MethodConceptKey { get; set; }

        /// <summary>
        /// Gets or sets the approach body site or system
        /// </summary>
        [Column("apr_ste_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid? ApproachSiteConceptKey { get; set; }

        /// <summary>
        /// Gets or sets the target site code
        /// </summary>
        [Column("trg_ste_cd_id"), ForeignKey(typeof(DbConcept), nameof(DbConcept.Key))]
        public Guid? TargetSiteConceptKey { get; set; }
    }
}
