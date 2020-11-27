/*
 * Copyright (C) 2019 - 2020, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
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
 * Date: 2019-11-27
 */
using System;

namespace SanteDB.OrmLite.Providers
{
    /// <summary>
    /// Represents features of SQL engine
    /// </summary>
    [Flags]
    public enum SqlEngineFeatures
    {
        None = 0x0,
        ReturnedInsertsAsReader = 0x1,
        AutoGenerateGuids = 0x2,
        AutoGenerateTimestamps = 0x4,
        LimitOffset = 0x8,
        FetchOffset = 0x10,
        ReturnedInsertsAsParms = 0x20,
        StrictSubQueryColumnNames = 0x40,
        MustNameSubQuery = 0x80,
        SetTimeout = 0x100
    }
}