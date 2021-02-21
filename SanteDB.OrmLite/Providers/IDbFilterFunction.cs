/*
 * Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
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
 * Date: 2021-2-9
 */
using System;

namespace SanteDB.OrmLite.Providers
{

    /// <summary>
    /// Represents a filter function for database
    /// </summary>
    public interface IDbFilterFunction 
    {

        /// <summary>
        /// Get the provider for the filter function
        /// </summary>
        String Provider { get; }

        /// <summary>
        /// Gets the name of the filter function
        /// </summary>
        String Name { get; }

        /// <summary>
        /// Creates the SQL Statement which implements the filter
        /// </summary>
        /// <param name="current">The current SQLStatement</param>
        /// <param name="filterColumn">The column being filtered on</param>
        /// <param name="parms">The parameters to the function</param>
        /// <param name="operand">The provided operand on the query string</param>
        /// <returns>The constructed / updated SQLStatement</returns>
        SqlStatement CreateSqlStatement(SqlStatement current, String filterColumn, String[] parms, String operand, Type operandType);

    }
}
