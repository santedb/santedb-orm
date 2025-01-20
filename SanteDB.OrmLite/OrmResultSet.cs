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
using SanteDB.Core.i18n;
using SharpCompress;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Represents a result set from enumerable data
    /// </summary>
    /// <typeparam name="TData">The type of record this result set holds</typeparam>
    public class OrmResultSet<TData> : IEnumerable<TData>, IOrmResultSet
    {

        /// <summary>
        /// Get the SQL statement
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
            this.Statement = stmt;
            this.Context = context;
        }

        /// <summary>
        /// Modify the SqlStatment in this result set
        /// </summary>
        private OrmResultSet<TData> ModifyStatement(Func<SqlStatementBuilder, SqlStatementBuilder> modifierFn)
        {
            return new OrmResultSet<TData>(this.Context, modifierFn(this.Context.CreateSqlStatementBuilder(this.Statement)).Statement);
        }

        /// <summary>
        /// Instructs the reader to skip n records
        /// </summary>
        public OrmResultSet<TData> Skip(int n, bool removeExisting = true) => this.ModifyStatement((s) => s.Offset(n, removeExisting));

        /// <summary>
        /// Instructs the reader to take <paramref name="n"/> records
        /// </summary>
        /// <param name="n">The number of records to take</param>
        public OrmResultSet<TData> Take(int n, bool removeExisting = true) => this.ModifyStatement((s) => s.Limit(n, removeExisting));

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

        ///// <summary>
        ///// Extract the first statement from union
        ///// </summary>
        ///// <returns></returns>
        //private SqlStatementBuilder ExtractFirstFromUnionIntersect()
        //{
        //    var innerQuery = this.m_statementBuilder.BuildStatement();
        //    var unionMatch = Constants.ExtractUnionIntersectRegex.Match(innerQuery.SQL);
        //    if (unionMatch.Success)
        //    {
        //        return this.Context.CreateSqlStatement(unionMatch.Groups[1].Value);
        //    }
        //    else
        //    {
        //        return innerQuery;
        //    }
        //}

        ///// <summary>
        ///// Transforms all the sub-select statements in a UNION or INTERSECT
        ///// </summary>
        //[Obsolete("", false)]
        //private SqlStatementBuilder TransformAll(Func<SqlStatementBuilder, SqlStatementBuilder> transformer)
        //{
        //    var innerQuery = this.m_statementBuilder.BuildStatement();
        //    var unionMatch = Constants.ExtractUnionIntersectRegex.Match(innerQuery.SQL);
        //    if (unionMatch.Success)
        //    {
        //        SqlStatementBuilder retVal = new SqlStatementBuilder(this.Context.Provider.StatementFactory, "", innerQuery.Arguments.ToArray());
        //        while (unionMatch.Success)
        //        {
        //            // Transform the first match
        //            retVal = retVal.Append(transformer(this.Context.CreateSqlStatement(unionMatch.Groups[1].Value.Trim()))).Append($" {unionMatch.Groups[2].Value} ");
        //            var secondStatement = unionMatch.Groups[3].Value;
        //            unionMatch = Constants.ExtractUnionIntersectRegex.Match(secondStatement);
        //            // If union match is successful we have a UNION b UNION c
        //            if (!unionMatch.Success) // no more intersects
        //            {
        //                retVal = retVal.Append(transformer(this.Context.CreateSqlStatement(secondStatement.Trim())));
        //            }
        //        }
        //        return retVal;
        //    }
        //    else
        //    {
        //        return transformer(innerQuery);
        //    }
        //}

        /// <summary>
        /// Instructs the reader to order by specified records
        /// </summary>
        /// <param name="sortFieldSelector">The field to order the results by</param>
        public OrmResultSet<TData> OrderBy(Expression<Func<TData, dynamic>> sortFieldSelector)
        {
            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
            if (sqlParts.Success)
            {
                var stmt = this.Context.CreateSqlStatementBuilder(this.Statement)
                    .WrapAsSubQuery()
                    .OrderBy(sortFieldSelector, Core.Model.Map.SortOrderType.OrderBy)
                    .Statement;
                return new OrmResultSet<TData>(this.Context, stmt);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(OrderBy)));
            }
        }

        /// <summary>
        /// Instructs the reader to order by specified records
        /// </summary>
        /// <param name="orderSelector">The selector to order by </param>
        public OrmResultSet<TData> OrderByDescending(Expression<Func<TData, dynamic>> orderSelector)
        {
            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
            if (sqlParts.Success)
            {
                var stmt = this.Context.CreateSqlStatementBuilder(this.Statement)
                    .WrapAsSubQuery()
                    .OrderBy(orderSelector, Core.Model.Map.SortOrderType.OrderByDescending)
                    .Statement;
                return new OrmResultSet<TData>(this.Context, stmt);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(OrderByDescending)));
            }
        }

        /// <summary>
        /// Return the first object
        /// </summary>
        public TData First()
        {
            TData retVal = this.FirstOrDefault();
            if (retVal == null)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

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

            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
            if (!sqlParts.Success)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Distinct)));
            }
            var stmt = this.Context.CreateSqlStatementBuilder(this.Statement)
                    .WrapAsSubQuery()
                    .Statement;
            return new OrmResultSet<TData>(this.Context, stmt);

        }


        /// <summary>
        /// Gets the specified keys from the object
        /// </summary>
        public OrmResultSet<T> Keys<T>(bool qualifyKeyTableName = true)
        {
            if (typeof(T) == typeof(TData))
            {
                return new OrmResultSet<T>(this.Context, this.Statement);
            }
            else
            {
                var tm = TableMapping.Get(typeof(TData));
                if (tm.TableName.StartsWith("CompositeResult"))
                {
                    tm = TableMapping.Get(typeof(TData).GetGenericArguments().Last());
                }
                if (tm.PrimaryKey.Count() != 1)
                {
                    throw new InvalidOperationException("Cannot execute KEY query on object with no keys");
                }

                // HACK: Swap out SELECT * if query starts with it
                var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(Statement.ToString());
                if (!sqlParts.Success)
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Keys)));
                }
                else
                {

                    var newStmt = this.Context.CreateSqlStatementBuilder(this.Statement)
                        .WrapAsSubQuery(tm.PrimaryKey.ToArray())
                        .Statement;
                    return new OrmResultSet<T>(this.Context, newStmt);
                }
            }
        }

        /// <summary>
        /// Correct the where clause
        /// </summary>
        private string CorrectWhereClause(string value) => String.IsNullOrEmpty(value) ? "true" : value;

        /// <summary>
        /// Get only those objects with the specified keys
        /// </summary>
        public IOrmResultSet HavingKeys(IEnumerable keyList, string keyColumnName = null)
        {
            ColumnMapping keyColumn = null;
            if (typeof(CompositeResult).IsAssignableFrom(typeof(TData)))
            {
                if (String.IsNullOrEmpty(keyColumnName))
                {
                    keyColumn = TableMapping.Get(typeof(TData).GetGenericArguments().Last()).PrimaryKey.Single();
                }
                else
                {
                    keyColumn = typeof(TData).GetGenericArguments().Select(o => TableMapping.Get(o).GetColumn(keyColumnName)).OfType<ColumnMapping>().First();
                }
            }
            else
            {
                keyColumn = String.IsNullOrEmpty(keyColumnName) ? TableMapping.Get(typeof(TData)).PrimaryKey.First() :
                    TableMapping.Get(typeof(TData)).GetColumn(keyColumnName);
            }

            // Union is only permitted in limited contexts
            var stmt = this.Statement.Prepare();
            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(stmt.ToString());
            if (sqlParts.Success)
            {
                if (stmt.ToString().ToLower().Contains(" union ") || stmt.ToString().Contains(" intersect "))
                {
                    var selectColumns = Constants.ExtractColumnBindingRegex.Replace(sqlParts.Groups[Constants.SQL_GROUP_COLUMNS].Value, (o) => $"{o.Groups[2].Value}{o.Groups[3].Value}");
                    var sqlStatementBuilder = this.Context.CreateSqlStatementBuilder($"SELECT {selectColumns} FROM (")
                        .Append($"WITH cte{keyColumn.Table.TableName} AS (").Append(stmt).Append($") ");
                    var keyArray = keyList.OfType<Object>();
                    var offset = 0;

                    do
                    {
                        sqlStatementBuilder.Append($" SELECT * FROM cte{keyColumn.Table.TableName} WHERE ").Append("FALSE").Append(" OR ");
                        var keyBatch = keyArray.Skip(offset).Take(500);
                        if (keyBatch.Any())
                        {
                            sqlStatementBuilder.Append(String.Join(" OR ", keyBatch.Select(o => $" cte{keyColumn.Table.TableName}.{keyColumn.Name} = ? ")), keyBatch.ToArray());
                            sqlStatementBuilder.Append(this.Context.Provider.StatementFactory.CreateSqlKeyword(Providers.SqlKeyword.UnionAll));
                        }
                        else if (offset > 0)
                        {
                            sqlStatementBuilder.RemoveLast(out _).RemoveLast(out _).RemoveLast(out _);
                        }
                        offset += 500;
                    } while (offset < keyArray.Count());


                    sqlStatementBuilder.RemoveLast(out _).Append($") AS {keyColumn.Table.TableName}");

                    return new OrmResultSet<TData>(this.Context, sqlStatementBuilder.Statement);
                }
                else
                {
                    // Is the inner query a sub-query?
                    if (sqlParts.Groups[Constants.SQL_GROUP_FROM].Value.TrimStart().StartsWith("("))
                    {
                        return new OrmResultSet<TData>(this.Context,
                            new SqlStatement(stmt.Alias, $"{sqlParts.Groups[Constants.SQL_GROUP_FROM].Value.TrimStart().Substring(1)} WHERE {sqlParts.Groups[Constants.SQL_GROUP_WHERE].Value}".Replace($") AS {stmt.Alias}", ""), stmt.Arguments)
                        ).HavingKeys(keyList, keyColumnName);
                    }

                    var offset = 0;
                    var keyArray = keyList.OfType<Object>();
                    var sqlStatementBuilder = this.Context.CreateSqlStatementBuilder();
                    while (offset <= keyArray.Count())
                    {

                        sqlStatementBuilder.Append($"SELECT {sqlParts.Groups[Constants.SQL_GROUP_COLUMNS].Value} FROM {sqlParts.Groups[Constants.SQL_GROUP_FROM].Value} WHERE ").Append("FALSE").Append(" OR ");

                        var keyBatch = keyArray.Skip(offset).Take(500);
                        if (keyBatch.Any())
                        {
                            sqlStatementBuilder.Append(String.Join(" OR ", keyBatch.Select(o => $" {keyColumn.Table.TableName}.{keyColumn.Name} = ? ")), keyBatch.ToArray());
                            sqlStatementBuilder.Append(this.Context.Provider.StatementFactory.CreateSqlKeyword(Providers.SqlKeyword.UnionAll));
                        }
                        else if (offset > 0)
                        {
                            sqlStatementBuilder.RemoveLast(out _).RemoveLast(out _).RemoveLast(out _);
                        }
                        offset += 500;

                    }

                    sqlStatementBuilder.RemoveLast(out _).UsingAlias(stmt.Alias).WrapAsSubQuery();

                    return new OrmResultSet<TData>(this.Context, sqlStatementBuilder.Statement);
                }
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(HavingKeys)));
            }
        }


        /// <summary>
        /// Select the specified column
        /// </summary>
        public OrmResultSet<T> Select<T>(Expression<Func<TData, T>> column)
        {
            var mapping = TableMapping.Get(typeof(TData)).GetColumn(column.Body.GetMember());
            var sqlParts = Constants.ExtractColumnBindingRegex.Match(this.Statement.ToString());
            if (sqlParts.Success)
            {
                var newStmt = this.Context.CreateSqlStatementBuilder($"SELECT {sqlParts.Groups[Constants.SQL_GROUP_DISTINCT].Value} {mapping.Name} AS v FROM (")
                    .Append(this.Statement)
                    .Append(") AS I")
                    .UsingAlias("I")
                    .Statement;
                return new OrmResultSet<T>(this.Context, newStmt);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Select)));
            }

        }

        /// <summary>
        /// Get the maximum value of the specifed column
        /// </summary>
        public T Max<T>(Expression<Func<TData, T>> column)
        {
            var mapping = TableMapping.Get(typeof(TData)).GetColumn(column.Body.GetMember());
            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
            if (!sqlParts.Success)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Max)));
            }
            else
            {
                var stmt = this.Context.CreateSqlStatementBuilder($"SELECT MAX({mapping.Name}) FROM (").Append(this.Statement).Append(") AS I").Statement;
                return this.Context.ExecuteScalar<T>(stmt);
            }
        }

        /// <summary>
        /// Get the maximum value of the specifed column
        /// </summary>
        public T Min<T>(Expression<Func<TData, T>> column)
        {
            var mapping = TableMapping.Get(typeof(TData)).GetColumn(column.Body.GetMember());
            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
            if (!sqlParts.Success)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Min)));
            }
            else
            {
                var stmt = this.Context.CreateSqlStatementBuilder($"SELECT MIN({mapping.Name}) FROM (").Append(this.Statement).Append(") AS I").Statement;
                return this.Context.ExecuteScalar<T>(stmt);
            }
        }

        /// <summary>
        /// Select the specified column
        /// </summary>
        public OrmResultSet<dynamic> Select(params Expression<Func<TData, dynamic>>[] columns)
        {
            var mapping = columns.Select(o => TableMapping.Get(typeof(TData)).GetColumn(o.Body.GetMember()));
            var sqlParts = Constants.ExtractColumnBindingRegex.Match(this.Statement.ToString());
            if (sqlParts.Success)
            {
                var newStmt = this.Context.CreateSqlStatementBuilder($"SELECT {sqlParts.Groups[Constants.SQL_GROUP_DISTINCT].Value} {String.Join(",", mapping.Select(o => o.Name))} AS v FROM (")
                    .Append(this.Statement)
                    .Append(") AS I")
                    .UsingAlias("I")
                    .Statement;
                return new OrmResultSet<dynamic>(this.Context, newStmt);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Select)));
            }
        }

        /// <summary>
        /// Gets the specified keys from the specified object type
        /// </summary>
        public IEnumerable<T> Keys<TKeyTable, T>()
        {
            var tm = TableMapping.Get(typeof(TKeyTable));

            if (tm.PrimaryKey.Count() != 1)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.DATA_STRUCTURE_NOT_APPROPRIATE, nameof(Keys), "nokeys"));
            }

            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
            if (!sqlParts.Success)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Keys)));
            }
            else
            {
                var newStmt = this.Context.CreateSqlStatementBuilder($"SELECT {sqlParts.Groups[Constants.SQL_GROUP_DISTINCT].Value} {String.Join(",", tm.PrimaryKey.Select(o => o.Name))} FROM (")
                    .Append(this.Statement)
                    .Append(") AS I")
                    .UsingAlias("I")
                    .Statement;
                return new OrmResultSet<T>(this.Context, newStmt);
            }
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
        /// Intersect the data
        /// </summary>
        public OrmResultSet<TData> Union(OrmResultSet<TData> other)
        {
            var sql = this.Statement + " UNION " + other.Statement;
            return new OrmResultSet<TData>(this.Context, sql);
        }

        /// <summary>
        /// Except the data
        /// </summary>
        public OrmResultSet<TData> Except(OrmResultSet<TData> other)
        {
            var sql = this.Statement + " EXCEPT " + other.Statement;
            return new OrmResultSet<TData>(this.Context, sql);
        }

        /// <summary>
        /// Intersect the data
        /// </summary>
        public OrmResultSet<TData> Intersect(OrmResultSet<TData> other)
        {
            var sqlStatement = this.Statement;
            if (this.Statement.Contains("EXCEPT")) // we need to wrap
            {
                sqlStatement = this.Context.CreateSqlStatementBuilder(this.Statement)
                        .WrapAsSubQuery()
                        .Append(" INTERSECT ")
                        .Append(other.Statement)
                        .Statement;
            }
            else
            {
                sqlStatement = this.Statement + " INTERSECT " + other.Statement;
            }
            return new OrmResultSet<TData>(this.Context, sqlStatement);
        }

        /// <summary>
        /// Union of other result set with this
        /// </summary>
        public IOrmResultSet Union(IOrmResultSet other)
        {
            return this.Union((OrmResultSet<TData>)other);
        }

        /// <summary>
        /// Except of other result set with this
        /// </summary>
        public IOrmResultSet Except(IOrmResultSet other)
        {
            return this.Except((OrmResultSet<TData>)other);
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
        public IOrmResultSet OrderBy(LambdaExpression orderExpression)
        {
            if (orderExpression is Expression<Func<TData, dynamic>> expr)
            {
                return this.OrderBy(expr);
            }
            else if ((typeof(CompositeResult).IsAssignableFrom(typeof(TData)) &&
                typeof(TData).GetGenericArguments().Contains(orderExpression.Parameters[0].Type) ||
                typeof(TData) == orderExpression.Parameters[0].Type)) // This is a composite result - so we want to know if any of the composite objects are TData
            {
                var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
                if (sqlParts.Success)
                {
                    var stmt = this.Context.CreateSqlStatementBuilder(this.Statement)
                        .WrapAsSubQuery()
                        .OrderBy(orderExpression, Core.Model.Map.SortOrderType.OrderBy)
                        .Statement;
                    return new OrmResultSet<TData>(this.Context, stmt);
                }
                else
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(OrderBy)));
                }
            }
            else if (typeof(ExpandoObject).IsAssignableFrom(typeof(TData)) &&
                orderExpression.Body is DynamicExpression me &&
                me.Binder is System.Dynamic.GetMemberBinder dmob)
            {
                var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
                if (sqlParts.Success)
                {
                    var stmt = this.Context.CreateSqlStatementBuilder(this.Statement)
                        .WrapAsSubQuery(ColumnMapping.Star)
                        .Append($" ORDER BY {dmob.Name}")
                        .Statement;
                    return new OrmResultSet<TData>(this.Context, stmt);
                }
                else
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(OrderBy)));
                }

            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, orderExpression.GetType(), typeof(Expression<Func<TData, Boolean>>)));
            }
        }

        /// <summary>
        /// Order by descending
        /// </summary>
        public IOrmResultSet OrderByDescending(LambdaExpression orderExpression)
        {
            if (orderExpression is Expression<Func<TData, dynamic>> expr)
            {
                return this.OrderByDescending(expr);
            }
            else if (orderExpression is LambdaExpression le &&
                (typeof(CompositeResult).IsAssignableFrom(typeof(TData)) &&
                typeof(TData).GetGenericArguments().Contains(le.Parameters[0].Type) ||
                typeof(TData) == le.Parameters[0].Type)) // This is a composite result - so we want to know if any of the composite objects are TData
            {

                var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
                if (sqlParts.Success)
                {
                    var stmt = this.Context.CreateSqlStatementBuilder(this.Statement)
                        .WrapAsSubQuery(ColumnMapping.Star)
                        .OrderBy(orderExpression, Core.Model.Map.SortOrderType.OrderByDescending)
                        .Statement;
                    return new OrmResultSet<TData>(this.Context, stmt);
                }
                else
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(OrderByDescending)));
                }
            }
            else if (typeof(ExpandoObject).IsAssignableFrom(typeof(TData)) &&
                orderExpression.Body is DynamicExpression me &&
                me.Binder is System.Dynamic.GetMemberBinder dmob)
            {
                var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
                if (sqlParts.Success)
                {
                    var stmt = this.Context.CreateSqlStatementBuilder(this.Statement)
                        .WrapAsSubQuery()
                        .Append($" ORDER BY {dmob.Name} DESC")
                        .Statement;
                    return new OrmResultSet<TData>(this.Context, stmt);
                }
                else
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(OrderByDescending)));
                }

            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, orderExpression.GetType(), typeof(Expression<Func<TData, dynamic>>)));
            }
        }

        /// <summary>
        /// A non-genericized select statement
        /// </summary>
        public OrmResultSet<TElement> Select<TElement>(String field)
        {
            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
            if (!sqlParts.Success)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Select)));
            }
            else if (typeof(CompositeResult).IsAssignableFrom(typeof(TData)))
            {
                // Get the appropriate mapping for the TData field
                foreach (var itm in typeof(TData).GetGenericArguments())
                {
                    var mapping = TableMapping.Get(itm).GetColumn(field);
                    if (mapping != null)
                    {
                        var newStmt = this.Context.CreateSqlStatementBuilder($"SELECT {sqlParts.Groups[Constants.SQL_GROUP_DISTINCT].Value} {mapping.Name} FROM (")
                            .Append(this.Statement)
                            .Append(") AS I")
                            .UsingAlias("I")
                            .Statement;
                        return new OrmResultSet<TElement>(this.Context, newStmt);
                    }
                }
                throw new InvalidOperationException(String.Format(ErrorMessages.FIELD_NOT_FOUND, field));
            }
            else
            {
                var mapping = TableMapping.Get(typeof(TData)).GetColumn(field);
                var newStmt = this.Context.CreateSqlStatementBuilder($"SELECT {sqlParts.Groups[Constants.SQL_GROUP_DISTINCT].Value} {mapping.Name} FROM (")
                    .Append(this.Statement)
                    .Append(") AS I")
                    .UsingAlias("I")
                    .Statement;
                return new OrmResultSet<TElement>(this.Context, newStmt);
            }
        }

        /// <summary>
        /// Distinct objects only
        /// </summary>
        IOrmResultSet IOrmResultSet.Distinct() => this.Distinct();

        /// <summary>
        /// Filter expression with a SELECT * FROM (XXXXXX) WHERE
        /// </summary>
        public IOrmResultSet Where(Expression whereExpression)
        {
            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
            if (whereExpression is LambdaExpression le &&
               (typeof(CompositeResult).IsAssignableFrom(typeof(TData)) &&
               typeof(TData).GetGenericArguments().Contains(le.Parameters[0].Type) ||
               typeof(TData) == le.Parameters[0].Type)) // This is a composite result - so we want to know if any of the composite objects are TData
            {
                var stmt = this.Context.CreateSqlStatementBuilder(this.Statement)
                    .WrapAsSubQuery()
                    .Where(le)
                    .Statement;
                return new OrmResultSet<TData>(this.Context, stmt);
            }
            else if (!sqlParts.Success)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(OrderBy)));
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, typeof(Expression<Func<TData, bool>>), whereExpression.GetType()));
            }

        }

        /// <summary>
        /// Remove ordering
        /// </summary>
        public IOrmResultSet WithoutOrdering(out SqlStatement orderByStatement)
        {
            var builder = this.Context.CreateSqlStatementBuilder(this.Statement).RemoveOrderBy(out orderByStatement);
            return new OrmResultSet<TData>(this.Context, builder.Statement);
        }

        /// <summary>
        /// Remove the skip
        /// </summary>
        public IOrmResultSet WithoutSkip(out int originalSkip)
        {
            var newStmt = this.Context.CreateSqlStatementBuilder(this.Statement).RemoveOffset(out originalSkip);
            var retVal = new OrmResultSet<TData>(this.Context, newStmt.Statement);
            return retVal;
        }

        /// <summary>
        /// Remove the take instruction
        /// </summary>
        public IOrmResultSet WithoutTake(out int originalTake)
        {
            var newStmt = this.Context.CreateSqlStatementBuilder(this.Statement).RemoveLimit(out originalTake);
            var retVal = new OrmResultSet<TData>(this.Context, newStmt.Statement);
            return retVal;
        }

        /// <inheritdoc/>
        public IOrmResultSet Clone(SqlStatement withStatement)
        {
            return new OrmResultSet<TData>(this.Context, withStatement);
        }

        /// <inheritdoc/>
        public IOrmResultSet CloneOnContext(DataContext context)
        {
            return new OrmResultSet<TData>(context, this.Statement);
        }
    }
}