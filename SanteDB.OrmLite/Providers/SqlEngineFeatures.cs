﻿/*
 * Copyright (C) 2021 - 2025, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
using System;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents features of SQL engine
    /// </summary>
    [Flags]
    public enum SqlEngineFeatures
    {
        /// <summary>
        /// The sql engine has no features
        /// </summary>
        None = 0x0,
        /// <summary>
        /// The sql engine returns the INSERT A RETURNING B as a data reader
        /// </summary>
        ReturnedInsertsAsReader = 0x1,
        /// <summary>
        /// The sql engine can auto-generate UUIDS when the <see cref="AutoGeneratedAttribute"/> is on a colum
        /// </summary>
        AutoGenerateGuids = 0x2,
        /// <summary>
        /// The sql engine can auto-generate TIMESTAMPS when the <see cref="AutoGeneratedAttribute"/> is on a DateTimeOffset column
        /// </summary>
        AutoGenerateTimestamps = 0x4,
        /// <summary>
        /// The sql engine uses standard OFFSET X ROWS LIMIT Y ROWS syntax
        /// </summary>
        LimitOffset = 0x8,
        /// <summary>
        /// The sql engine uses FETCH FIRST X ROWS ONLY LIMIT Y ROWS syntax
        /// </summary>
        FetchOffset = 0x10,
        /// <summary>
        /// The sql engine returns the INSERT A RETURNING B as a series of output parameters
        /// </summary>
        ReturnedInsertsAsParms = 0x20,
        /// <summary>
        /// The sql engine is strict about sub-query column names
        /// </summary>
        StrictSubQueryColumnNames = 0x40,
        /// <summary>
        /// The sql engine requires that sub-queries have names
        /// </summary>
        MustNameSubQuery = 0x80,
        /// <summary>
        /// The sql engine supports the SET TIMEOUT option
        /// </summary>
        SetTimeout = 0x100,
        /// <summary>
        /// The sql egine supports updates as a reader
        /// </summary>
        ReturnedUpdatesAsReader = 0x200,
        /// <summary>
        /// The sql engine supports materialized views
        /// </summary>
        MaterializedViews = 0x400,
        /// <summary>
        /// The SQL engine can auto-generate columns with <see cref="AutoGeneratedAttribute"/> of type int (sequences)
        /// </summary>
        AutoGenerateSequences = 0x800,
        /// <summary>
        /// The SQL engine supports the TRUNCATE command
        /// </summary>
        Truncate = 0x1000,
        /// <summary>
        /// The SQL engine supports TRUNCATE X CASCADE or DELETE FROM X CASCADE syntax
        /// </summary>
        Cascades = 0x2000,
        /// <summary>
        /// The SQL engine requires manual intervention of the freetext index
        /// </summary>
        StoredFreetextIndex = 0x4000
    }
}