/*
 * Copyright (C) 2021 - 2021, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-8-5
 */
using System;

namespace SanteDB.OrmLite.Attributes
{
    /// <summary>
    /// The skip hint attribute allows the more complex auto-joining 
    /// tools to understand when skipping a join can be performed
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class SkipHintAttribute : Attribute
    {

        /// <summary>
        /// Skip attribute
        /// </summary>
        public SkipHintAttribute(string queryHint)
        {
            this.QueryHint = queryHint;
        }

        /// <summary>
        /// Gets the query path which , if not present in the query, indicates the class can be skipped
        /// </summary>
        public String QueryHint { get; }
    }
}
