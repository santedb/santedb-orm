/*
 * Copyright (C) 2021 - 2024, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 */
using System;
using System.Data;

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
        /// <param name="currentBuilder">The current builder on which the SQL statement can be modified</param>
        /// <param name="filterColumn">The column being filtered on</param>
        /// <param name="parms">The parameters to the function</param>
        /// <param name="operand">The provided operand on the query string</param>
        /// <param name="targetProperty">The type to interpret the <paramref name="parms"/> as before passing to the function.</param>
        /// <returns>The constructed / updated SQLStatement</returns>
        SqlStatementBuilder CreateSqlStatement(SqlStatementBuilder currentBuilder, String filterColumn, String[] parms, String operand, Type operandType);

    }

    /// <summary>
    /// Represents a <see cref="IDbFilterFunction"/> which requires the loading of external libraries or setup
    /// </summary>
    public interface IDbInitializedFilterFunction : IDbFilterFunction
    {

        /// <summary>
        /// Initialize this filter function on <paramref name="connection"/>
        /// </summary>
        /// <param name="connection">The connection on which the DB filter function should be initialized</param>
        /// <returns>True if the initialization on the connection was successful</returns>
        bool Initialize(IDbConnection connection, IDbTransaction currentTransaction);

        /// <summary>
        /// Gets the order that this function should be initialized in when the connection is opened. The default is 0. Negative values are supported. This order does not affect the invocation order of <see cref="IDbFilterFunction.CreateSqlStatement(SqlStatementBuilder, string, string[], string, Type)"/>.
        /// </summary>
        int Order { get; }
    }
}
