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
 * Date: 2023-5-19
 */
using SanteDB.OrmLite.Attributes;
using SanteDB.Persistence.Data.ADO.Data.Model.Acts;
using SanteDB.Persistence.Data.ADO.Data.Model.Entities;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Security
{
    /// <summary>
    /// Represents a security policy instance which includes a link to a policy and
    /// to a decision
    /// </summary>
    [ExcludeFromCodeCoverage]
    public abstract class DbSecurityPolicyInstance : DbAssociation
    {
        /// <summary>
        /// Gets or sets the key
        /// </summary>
        [Column("sec_pol_inst_id"), PrimaryKey, AutoGenerated]
        public override Guid Key { get; set; }

        /// <summary>
        /// Gets or sets the policy identifier.
        /// </summary>
        /// <value>The policy identifier.</value>
        [Column("pol_id"), ForeignKey(typeof(DbSecurityPolicy), nameof(DbSecurityPolicy.Key))]
        public Guid PolicyKey
        {
            get;
            set;
        }

        /// <inheritdoc/>
        public override int GetHashCode() => this.SourceKey.GetHashCode() * this.PolicyKey.GetHashCode();

        /// <summary>
        /// Determine if A equals B
        /// </summary>
        public override bool Equals(object obj)
        {
            if (obj is DbSecurityPolicyInstance dbo)
            {
                return dbo.SourceKey == this.SourceKey && dbo.PolicyKey == this.PolicyKey;
            }
            else
            {
                return base.Equals(obj);
            }
        }
    }

    /// <summary>
    /// Represents a relationship between an entity and security policy
    /// </summary>
    [Table("ent_pol_assoc_tbl")]
    [ExcludeFromCodeCoverage]
    public class DbEntitySecurityPolicy : DbSecurityPolicyInstance, IDbVersionedAssociation
    {
        /// <summary>
        /// Gets or sets the source
        /// </summary>
        /// <value>The source identifier.</value>
        [Column("ent_id"), ForeignKey(typeof(DbEntity), nameof(DbEntity.Key))]
        public override Guid SourceKey
        {
            get;
            set;
        }

        /// <summary>
        /// Effective version sequence
        /// </summary>
        [Column("efft_vrsn_seq_id"), NotNull]
        public Int64 EffectiveVersionSequenceId { get; set; }

        /// <summary>
        /// Obsolete version sequence ID
        /// </summary>
        [Column("obslt_vrsn_seq_id")]
        public Int64? ObsoleteVersionSequenceId { get; set; }
    }

    /// <summary>
    /// Represents a security policy applied to an act
    /// </summary>
    [ExcludeFromCodeCoverage]
    [Table("act_pol_assoc_tbl")]
    public class DbActSecurityPolicy : DbSecurityPolicyInstance, IDbVersionedAssociation
    {
        /// <summary>
        /// Gets or sets the source
        /// </summary>
        /// <value>The source identifier.</value>
        [Column("act_id"), ForeignKey(typeof(DbAct), nameof(DbAct.Key))]
        public override Guid SourceKey
        {
            get;
            set;
        }

        /// <summary>
        /// Effective version sequence
        /// </summary>
        [Column("efft_vrsn_seq_id"), NotNull]
        public Int64 EffectiveVersionSequenceId { get; set; }

        /// <summary>
        /// Obsolete version sequence ID
        /// </summary>
        [Column("obslt_vrsn_seq_id")]
        public Int64? ObsoleteVersionSequenceId { get; set; }
    }

    /// <summary>
    /// Represents a security policy applied to a role
    /// </summary>
    [ExcludeFromCodeCoverage]
    [Table("sec_rol_pol_assoc_tbl")]
    public class DbSecurityRolePolicy : DbSecurityPolicyInstance
    {
        /// <summary>
        /// Gets or sets the type of the grant.
        /// </summary>
        /// <value>The type of the grant.</value>
        [Column("pol_act")]
        public int GrantType
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the source
        /// </summary>
        /// <value>The source identifier.</value>
        [Column("rol_id"), ForeignKey(typeof(DbSecurityRole), nameof(DbSecurityRole.Key))]
        public override Guid SourceKey
        {
            get;
            set;
        }
    }

    /// <summary>
    /// Represents a security policy applied to an application (this is "my" data)
    /// </summary>
    [ExcludeFromCodeCoverage]
    [Table("sec_app_pol_assoc_tbl")]
    public class DbSecurityApplicationPolicy : DbSecurityPolicyInstance
    {
        /// <summary>
        /// Gets or sets the source
        /// </summary>
        /// <value>The source identifier.</value>
        [Column("app_id"), ForeignKey(typeof(DbSecurityApplication), nameof(DbSecurityApplication.Key))]
        public override Guid SourceKey
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the type of the grant.
        /// </summary>
        /// <value>The type of the grant.</value>
        [Column("pol_act")]
        public int GrantType
        {
            get;
            set;
        }
    }

    /// <summary>
    /// Represents a security policy applied to a device
    /// </summary>
    [ExcludeFromCodeCoverage]
    [Table("sec_dev_pol_assoc_tbl")]
    public class DbSecurityDevicePolicy : DbSecurityPolicyInstance
    {
        /// <summary>
        /// Gets or sets the source
        /// </summary>
        /// <value>The source identifier.</value>
        [Column("dev_id"), ForeignKey(typeof(DbSecurityDevice), nameof(DbSecurityDevice.Key))]
        public override Guid SourceKey
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the type of the grant.
        /// </summary>
        /// <value>The type of the grant.</value>
        [Column("pol_act")]
        public int GrantType
        {
            get;
            set;
        }
    }
}