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
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Entities
{
    /// <summary>
    /// User entity ORM
    /// </summary>
    [Table("usr_ent_tbl")]
    public class DbUserEntity : DbPersonSubTable
    {

        /// <summary>
        /// Gets or sets the security user which is associated with this entity
        /// </summary>
        [Column("sec_usr_id"), ForeignKey(typeof(DbSecurityUser), nameof(DbSecurityUser.Key))]
        public Guid SecurityUserKey { get; set; }

    }
}
