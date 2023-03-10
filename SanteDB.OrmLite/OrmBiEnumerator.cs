/*
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
using System;
using System.Collections;
using System.Collections.Generic;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// ORM BI enumerator
    /// </summary>
    internal class OrmBiEnumerator : IEnumerable<object>
    {
        private readonly IOrmResultSet m_ormResultSet;

        /// <summary>
        /// Result set of the ORM enumerator
        /// </summary>
        public OrmBiEnumerator(IOrmResultSet ormResultSet)
        {
            this.m_ormResultSet = ormResultSet;
        }

        /// <summary>
        /// Get the enumerator
        /// </summary>
        public IEnumerator<object> GetEnumerator()
        {
            using(var context = this.m_ormResultSet.Context.OpenClonedContext())
            {
                context.Open();
                foreach(var itm in this.m_ormResultSet.CloneOnContext(context))
                {
                    yield return itm;
                }
            }
        }

        /// <summary>
        /// Get enumerator
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
    }
}