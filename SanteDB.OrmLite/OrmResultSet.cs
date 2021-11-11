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

using SanteDB.Core.i18n;
using SanteDB.Core.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Represents a result set from enumerable data
    /// </summary>
    /// <typeparam name="TData">The type of record this result set holds</typeparam>
    public class OrmResultSet<TData> : IEnumerable<TData>, IOrmResultSet
    {
        /// <summary>
        /// Gets the SQL statement that this result set is based on
        /// </summary>
        public SqlStatement Statement { get; }

        /// <summary>
        /// Get the context
        /// </summary>
        public DataContext Context { get; }

        /// <summary>
        /// Element type
        /// </summary>
        public Type ElementType => typeof(TData);

        /// <summary>
        /// Create a new result set based on the context and statement
        /// </summary>
        /// <param name="stmt">The SQL Statement</param>
        /// <param name="context">The data context on which data should be executed</param>
        internal OrmResultSet(DataContext context, SqlStatement stmt)
        {
            this.Statement = stmt.Build();
            this.Context = context;
        }

        /// <summary>
        /// Instructs the reader to skip n records
        /// </summary>
        public OrmResultSet<TData> Skip(int n)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().Offset(n));
        }

        /// <summary>
        /// Instructs the reader to take <paramref name="n"/> records
        /// </summary>
        /// <param name="n">The number of records to take</param>
        public OrmResultSet<TData> Take(int n)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().Limit(n));
        }

        /// <summary>
        /// Instructs the reader to count the number of records
        /// </summary>
        public int Count()
        {
            return this.Context.Count(this.Statement);
        }

        /// <summary>
        /// Instructs the reader to count the number of records
        /// </summary>
        public bool Any()
        {
            return this.Context.Any(this.Statement);
        }

        /// <summary>
        /// Instructs the reader to order by specified records
        /// </summary>
        /// <param name="selector">The key to order by</param>
        public OrmResultSet<TData> OrderBy(Expression<Func<TData, dynamic>> keySelector)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().OrderBy<TData>(keySelector, Core.Model.Map.SortOrderType.OrderBy));
        }

        /// <summary>
        /// Instructs the reader to order by specified records
        /// </summary>
        /// <param name="selector">The selector to order by </param>
        public OrmResultSet<TData> OrderByDescending(Expression<Func<TData, dynamic>> keySelector)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().OrderBy<TData>(keySelector, Core.Model.Map.SortOrderType.OrderByDescending));
        }

        /// <summary>
        /// Return the first object
        /// </summary>
        public TData First()
        {
            TData retVal = this.FirstOrDefault();
            if (retVal == null)
                throw new InvalidOperationException("Sequence contains no elements");
            return retVal;
        }

        /// <summary>
        /// Return the first object
        /// </summary>
        public TData FirstOrDefault()
        {
            return this.Context.FirstOrDefault<TData>(this.Statement);
        }

        /// <summary>
        /// Get the enumerator
        /// </summary>
        public IEnumerator<TData> GetEnumerator()
        {
            return this.Context.ExecQuery<TData>(this.Statement).GetEnumerator();
        }

        /// <summary>
        /// Get the non-generic enumerator
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.Context.ExecQuery<TData>(this.Statement).GetEnumerator();
        }

        /// <summary>
        /// Select distinct records
        /// </summary>
        public OrmResultSet<TData> Distinct()
        {
            var innerQuery = this.Statement.Build();
            if (!this.Statement.SQL.StartsWith("SELECT DISTINCT"))
                return new OrmResultSet<TData>(this.Context, this.Context.CreateSqlStatement($"SELECT DISTINCT {innerQuery.SQL.Substring(7)}", innerQuery.Arguments.ToArray()));
            else
                return this;
        }

        /// <summary>
        /// Gets the specified keys from the object
        /// </summary>
        public OrmResultSet<T> Keys<T>(bool qualifyKeyTableName = true)
        {
            if (typeof(T) == typeof(TData))
                return new OrmResultSet<T>(this.Context, this.Statement);
            else
            {
                var innerQuery = this.Statement.Build();
                var tm = TableMapping.Get(typeof(TData));
                if (tm.TableName.StartsWith("CompositeResult"))
                {
                    tm = TableMapping.Get(typeof(TData).GetGenericArguments().Last());
                }
                if (tm.PrimaryKey.Count() != 1)
                    throw new InvalidOperationException("Cannot execute KEY query on object with no keys");

                // HACK: Swap out SELECT * if query starts with it
                if (innerQuery.SQL.StartsWith("SELECT * "))
                {
                    if (qualifyKeyTableName)
                        innerQuery = this.Context.CreateSqlStatement($"SELECT {tm.TableName}.{tm.PrimaryKey.First().Name} {innerQuery.SQL.Substring(9)}", innerQuery.Arguments.ToArray());
                    else
                        innerQuery = this.Context.CreateSqlStatement($"SELECT {tm.PrimaryKey.First().Name} {innerQuery.SQL.Substring(9)}", innerQuery.Arguments.ToArray());
                }

                return new OrmResultSet<T>(this.Context, this.Context.CreateSqlStatement($"SELECT {String.Join(",", tm.PrimaryKey.Select(o => o.Name))} FROM (").Append(innerQuery.Build()).Append(") AS I"));
            }
        }

        /// <summary>
        /// Select the specified column
        /// </summary>
        public OrmResultSet<T> Select<T>(Expression<Func<TData, T>> column)
        {
            var mapping = TableMapping.Get(typeof(TData)).GetColumn(column.Body.GetMember());
            return new OrmResultSet<T>(this.Context, this.Context.CreateSqlStatement($"SELECT I.{mapping.Name} V FROM (").Append(this.Statement.Build()).Append(") AS I"));
        }

        /// <summary>
        /// Get the maximum value of the specifed column
        /// </summary>
        public T Max<T>(Expression<Func<TData, T>> column)
        {
            var mapping = TableMapping.Get(typeof(TData)).GetColumn(column.Body.GetMember());
            return this.Context.ExecuteScalar<T>(this.Context.CreateSqlStatement($"SELECT MAX({mapping.Name}) FROM (").Append(this.Statement.Build()).Append(") AS I"));
        }

        /// <summary>
        /// Get the maximum value of the specifed column
        /// </summary>
        public T Min<T>(Expression<Func<TData, T>> column)
        {
            var mapping = TableMapping.Get(typeof(TData)).GetColumn(column.Body.GetMember());
            return this.Context.ExecuteScalar<T>(this.Context.CreateSqlStatement($"SELECT MIN({mapping.Name}) FROM (").Append(this.Statement.Build()).Append(") AS I"));
        }

        /// <summary>
        /// Select the specified column
        /// </summary>
        public OrmResultSet<dynamic> Select(params Expression<Func<TData, dynamic>>[] columns)
        {
            var mapping = TableMapping.Get(typeof(TData));
            return new OrmResultSet<dynamic>(this.Context, this.Context.CreateSqlStatement($"SELECT {String.Join(",", columns.Select(o => mapping.GetColumn(o.Body.GetMember())).Select(o => o.Name))} FROM (").Append(this.Statement.Build()).Append(") AS I"));
        }

        /// <summary>
        /// Gets the specified keys from the specified object type
        /// </summary>
        public IEnumerable<T> Keys<TKeyTable, T>()
        {
            var innerQuery = this.Statement.Build();
            var tm = TableMapping.Get(typeof(TKeyTable));

            if (tm.PrimaryKey.Count() != 1)
                throw new InvalidOperationException("Cannot execute KEY query on object with no keys");

            // HACK: Swap out SELECT * if query starts with it
            if (innerQuery.SQL.StartsWith("SELECT * "))
                innerQuery = this.Context.CreateSqlStatement($"SELECT {tm.TableName}.{tm.PrimaryKey.First().Name} {innerQuery.SQL.Substring(9)}", innerQuery.Arguments.ToArray());

            return new OrmResultSet<T>(this.Context, this.Context.CreateSqlStatement($"SELECT {String.Join(",", tm.PrimaryKey.Select(o => o.Name))} FROM (").Append(innerQuery.Build()).Append(") AS I"));
        }

        /// <summary>
        /// Gets the keys
        /// </summary>
        IOrmResultSet IOrmResultSet.Keys<TKey>()
        {
            return this.Keys<TKey>(false);
        }

        /// <summary>
        /// Gets the keys
        /// </summary>
        IOrmResultSet IOrmResultSet.Skip(int n)
        {
            return this.Skip(n);
        }

        /// <summary>
        /// Gets the keys
        /// </summary>
        IOrmResultSet IOrmResultSet.Take(int n)
        {
            return this.Take(n);
        }

        /// <summary>
        /// Convert to an SQL statement
        /// </summary>
        public SqlStatement ToSqlStatement()
        {
            return this.Statement.Build();
        }

        /// <summary>
        /// Intersect the data
        /// </summary>
        public OrmResultSet<TData> Union(OrmResultSet<TData> other)
        {
            var sql = this.ToSqlStatement();
            sql = sql.Append(" UNION ").Append(other.ToSqlStatement());
            return new OrmResultSet<TData>(this.Context, sql);
        }

        /// <summary>
        /// Union of other result set with this
        /// </summary>
        public IOrmResultSet Union(IOrmResultSet other)
        {
            return this.Union((OrmResultSet<TData>)other);
        }

        /// <summary>
        /// Intersect the other result set
        /// </summary>
        public IOrmResultSet Intersect(IOrmResultSet other)
        {
            return this.Intersect((OrmResultSet<TData>)other);
        }

        /// <summary>
        ///Get the first or default of the object in the result set
        /// </summary>
        object IOrmResultSet.FirstOrDefault() => this.FirstOrDefault();

        /// <summary>
        /// Order by the provided expression
        /// </summary>
        public IOrmResultSet OrderBy(Expression orderExpression)
        {
            if (orderExpression is Expression<Func<TData, dynamic>> expr)
                return this.OrderBy(expr);
            else
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, orderExpression.GetType(), typeof(Expression<Func<TData, Boolean>>)));
        }

        /// <summary>
        /// Order by descending
        /// </summary>
        public IOrmResultSet OrderByDescending(Expression orderExpression)
        {
            if (orderExpression is Expression<Func<TData, dynamic>> expr)
                return this.OrderByDescending(expr);
            else
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, orderExpression.GetType(), typeof(Expression<Func<TData, Boolean>>)));
        }

        /// <summary>
        /// A non-genericized select statement
        /// </summary>
        public OrmResultSet<TElement> Select<TElement>(String field)
        {
            if (typeof(CompositeResult).IsAssignableFrom(typeof(TData)))
            {
                // Get the appropriate mapping for the TData field
                foreach(var itm in typeof(TData).GetGenericArguments())
                {
                    var mapping = TableMapping.Get(itm).GetColumn(field);
                    if(mapping != null)
                    {
                        return new OrmResultSet<TElement>(this.Context, this.Context.CreateSqlStatement($"SELECT I.{mapping.Name} V FROM (").Append(this.Statement.Build()).Append(") AS I"));
                    }
                }
                throw new InvalidOperationException(String.Format(ErrorMessages.FIELD_NOT_FOUND, field));
            }
            else
            {
                var mapping = TableMapping.Get(typeof(TData)).GetColumn(field);
                return new OrmResultSet<TElement>(this.Context, this.Context.CreateSqlStatement($"SELECT I.{mapping.Name} V FROM (").Append(this.Statement.Build()).Append(") AS I"));
            }

            //var parm = Expression.Parameter(typeof(TData));
            //var filterExpression = Expression.Lambda<Func<TData, TElement>>(
            //    Expression.Convert(Expression.MakeMemberAccess(parm, typeof(TData).GetProperty(field)), typeof(TElement)),
            //    parm
            //    );
            //return this.Select<TElement>(filterExpression);
        }

        /// <summary>
        /// Distinct objects only
        /// </summary>
        IOrmResultSet IOrmResultSet.Distinct() => this.Distinct();
    }
}