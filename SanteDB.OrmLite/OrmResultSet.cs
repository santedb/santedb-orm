/*
 * Copyright (C) 2021 - 2022, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2022-5-30
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using SanteDB.Core.i18n;
using SanteDB.Core.Model;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Represents a result set from enumerable data
    /// </summary>
    /// <typeparam name="TData">The type of record this result set holds</typeparam>
    public class OrmResultSet<TData> : IEnumerable<TData>, IOrmResultSet
    {

        private readonly Regex m_extractUnionIntersects = new Regex(@"^(.*?)(UNION|INTERSECT|UNION ALL|INTERSECT ALL)(.*?)$");
        private readonly Regex m_extractRawSelectStatment = new Regex(@"^SELECT\s(DISTINCT)?(.*?)FROM(.*?)WHERE(.*?)((ORDER|OFFSET|LIMIT).*)?$");
        private const int SQL_GROUP_DISTINCT = 1;
        private const int SQL_GROUP_COLUMNS = 2;
        private const int SQL_GROUP_FROM = 3;
        private const int SQL_GROUP_WHERE = 4;
        private const int SQL_GROUP_LIMIT = 5;

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
            var stmt = this.Statement.Build().RemoveOffset(out _).RemoveLimit(out var limit).Offset(n);
            // Re-add limit
            if(limit >= 0)
            {
                stmt = stmt.Limit(limit);
            }
            return new OrmResultSet<TData>(this.Context, stmt.Build());
        }

        /// <summary>
        /// Instructs the reader to take <paramref name="n"/> records
        /// </summary>
        /// <param name="n">The number of records to take</param>
        public OrmResultSet<TData> Take(int n)
        {
            var stmt = this.Statement.Build().RemoveLimit(out _).Limit(n);
            return new OrmResultSet<TData>(this.Context, stmt);
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
        /// Transforms all the sub-select statements in a UNION or INTERSECT
        /// </summary>
        private SqlStatement TransformAll(Func<SqlStatement, SqlStatement> transformer)
        {
            var innerQuery = this.Statement.Build();
            var unionMatch = this.m_extractUnionIntersects.Match(innerQuery.SQL);
            if (unionMatch.Success)
            {
                SqlStatement retVal = new SqlStatement(this.Context.Provider, "", innerQuery.Arguments.ToArray());
                while (unionMatch.Success)
                {
                    // Transform the first match
                    retVal = retVal.Append(transformer(this.Context.CreateSqlStatement(unionMatch.Groups[1].Value.Trim()))).Append($" {unionMatch.Groups[2].Value} ");
                    var secondStatement = unionMatch.Groups[3].Value;
                    unionMatch = this.m_extractUnionIntersects.Match(secondStatement);
                    // If union match is successful we have a UNION b UNION c
                    if(!unionMatch.Success) // no more intersects
                    {
                        retVal = retVal.Append(transformer(this.Context.CreateSqlStatement(secondStatement.Trim())));
                    }
                }
                return retVal;
            }
            else {
                return transformer( innerQuery);
            }
        }

        /// <summary>
        /// Instructs the reader to order by specified records
        /// </summary>
        /// <param name="selector">The key to order by</param>
        public OrmResultSet<TData> OrderBy(Expression<Func<TData, dynamic>> keySelector)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().OrderBy(keySelector, Core.Model.Map.SortOrderType.OrderBy));
        }

        /// <summary>
        /// Instructs the reader to order by specified records
        /// </summary>
        /// <param name="selector">The selector to order by </param>
        public OrmResultSet<TData> OrderByDescending(Expression<Func<TData, dynamic>> keySelector)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().OrderBy(keySelector, Core.Model.Map.SortOrderType.OrderByDescending));
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
            return this.Context.FirstOrDefault<TData>(this.Statement.Build());
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
            return new OrmResultSet<TData>(this.Context,  this.TransformAll(stmt =>
            {
                var innerQuery = stmt.Build();
                var sqlParts = this.m_extractRawSelectStatment.Match(innerQuery.SQL);
                if (!sqlParts.Success)
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Distinct)));
                }
                else if (String.IsNullOrEmpty(sqlParts.Groups[SQL_GROUP_DISTINCT].Value)) // not already distcint
                {
                    return this.Context.CreateSqlStatement(
                        $"SELECT DISTINCT {sqlParts.Groups[SQL_GROUP_COLUMNS].Value} FROM {sqlParts.Groups[SQL_GROUP_FROM].Value} WHERE {sqlParts.Groups[SQL_GROUP_WHERE].Value} {sqlParts.Groups[SQL_GROUP_LIMIT].Value}",
                        innerQuery.Arguments.ToArray()
                        );
                }
                else
                    return this.Statement;
            }));
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
                return new OrmResultSet<T>(this.Context, this.TransformAll(stmt =>
                {
                    var innerQuery = stmt.Build();
                    var tm = TableMapping.Get(typeof(TData));
                    if (tm.TableName.StartsWith("CompositeResult"))
                    {
                        tm = TableMapping.Get(typeof(TData).GetGenericArguments().Last());
                    }
                    if (tm.PrimaryKey.Count() != 1)
                        throw new InvalidOperationException("Cannot execute KEY query on object with no keys");

                    // HACK: Swap out SELECT * if query starts with it
                    var sqlParts = this.m_extractRawSelectStatment.Match(innerQuery.SQL);
                    if (!sqlParts.Success)
                    {
                        throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Keys)));
                    }
                    else
                    {
                        return this.Context.CreateSqlStatement($"SELECT {sqlParts.Groups[SQL_GROUP_DISTINCT].Value} {String.Join(",", tm.PrimaryKey.Select(o => o.Name))} FROM {sqlParts.Groups[SQL_GROUP_FROM].Value} WHERE {sqlParts.Groups[SQL_GROUP_WHERE].Value} {sqlParts.Groups[SQL_GROUP_LIMIT].Value}", innerQuery.Arguments.ToArray());
                    }

                }));
            }
        }

        /// <summary>
        /// Get only those objects with the specified keys
        /// </summary>
        public IOrmResultSet HavingKeys(IEnumerable keyList, string keyColumnName = null)
        {

            
            ColumnMapping keyColumn = null;
            if (typeof(CompositeResult).IsAssignableFrom(typeof(TData)))
            {
                if(String.IsNullOrEmpty(keyColumnName))
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

            // HACK: Find a better way to dissassembly the query - basically we want to get the SELECT * FROM XXXX WHERE ----- and swap out the WHERE clause to only those keys in our set

            return new OrmResultSet<TData>(this.Context, this.TransformAll(stmt =>
            {
                var selectMatch = this.m_extractRawSelectStatment.Match(stmt.Build().SQL);
                if (!selectMatch.Success)
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(HavingKeys)));
                }

                var sqlStatement = this.Context.CreateSqlStatement($"SELECT {selectMatch.Groups[SQL_GROUP_DISTINCT].Value} {selectMatch.Groups[SQL_GROUP_COLUMNS].Value} FROM {selectMatch.Groups[SQL_GROUP_FROM].Value} WHERE ").Append("FALSE").Append(" OR ");

                foreach (var itm in keyList)
                {
                    sqlStatement = sqlStatement.Append($" {keyColumn.Table.TableName}.{keyColumn.Name} = ?", itm).Append(" OR ");
                }
                sqlStatement.RemoveLast();
                return sqlStatement.Append($" {selectMatch.Groups[SQL_GROUP_LIMIT]}").Build();
            }));
            
        }
        /// <summary>
        /// Select the specified column
        /// </summary>
        public OrmResultSet<T> Select<T>(Expression<Func<TData, T>> column)
        {
            return new OrmResultSet<T>(this.Context, this.TransformAll(stmt =>
            {
                var mapping = TableMapping.Get(typeof(TData)).GetColumn(column.Body.GetMember());
                var innerQuery = stmt.Build();
                var sqlParts = this.m_extractRawSelectStatment.Match(innerQuery.SQL);
                if (!sqlParts.Success)
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Select)));
                }
                else
                {
                    return this.Context.CreateSqlStatement($"SELECT {sqlParts.Groups[SQL_GROUP_DISTINCT].Value} {mapping.Table.TableName}.{mapping.Name} AS v FROM {sqlParts.Groups[SQL_GROUP_FROM].Value} WHERE {sqlParts.Groups[SQL_GROUP_WHERE].Value} {sqlParts.Groups[SQL_GROUP_LIMIT].Value}", innerQuery.Arguments.ToArray());
                }
            }));
        }

        /// <summary>
        /// Get the maximum value of the specifed column
        /// </summary>
        public T Max<T>(Expression<Func<TData, T>> column)
        {
            var mapping = TableMapping.Get(typeof(TData)).GetColumn(column.Body.GetMember());
            var innerQuery = this.Statement.Build();
            var sqlParts = this.m_extractRawSelectStatment.Match(innerQuery.SQL);
            if (!sqlParts.Success)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Max)));
            }
            else
            {
                return this.Context.ExecuteScalar<T>(this.Context.CreateSqlStatement($"SELECT MAX({mapping.Name}) FROM (").Append(innerQuery).Append(") I"));
            }
        }

        /// <summary>
        /// Get the maximum value of the specifed column
        /// </summary>
        public T Min<T>(Expression<Func<TData, T>> column)
        {
            var mapping = TableMapping.Get(typeof(TData)).GetColumn(column.Body.GetMember());
            var innerQuery = this.Statement.Build();
            var sqlParts = this.m_extractRawSelectStatment.Match(innerQuery.SQL);
            if (!sqlParts.Success)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Min)));
            }
            else
            {
                return this.Context.ExecuteScalar<T>(this.Context.CreateSqlStatement($"SELECT MIN({mapping.Name}) FROM (").Append(innerQuery).Append(") I"));
            }
        }

        /// <summary>
        /// Select the specified column
        /// </summary>
        public OrmResultSet<dynamic> Select(params Expression<Func<TData, dynamic>>[] columns)
        {

            var mapping = columns.Select(o=> TableMapping.Get(typeof(TData)).GetColumn(o.Body.GetMember()));

            return new OrmResultSet<dynamic>(this.Context, this.TransformAll(stmt =>
            {
                var innerQuery = stmt.Build();
                var sqlParts = this.m_extractRawSelectStatment.Match(innerQuery.SQL);
                if (!sqlParts.Success)
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Select)));
                }
                else
                {
                    return this.Context.CreateSqlStatement($"SELECT {sqlParts.Groups[SQL_GROUP_DISTINCT].Value} {String.Join(",", mapping.Select(o => $"{o.Table.TableName}.{o.Name}"))} FROM {sqlParts.Groups[SQL_GROUP_FROM].Value} WHERE {sqlParts.Groups[SQL_GROUP_WHERE].Value} {sqlParts.Groups[SQL_GROUP_LIMIT].Value}", innerQuery.Arguments.ToArray());
                }
            }));
        }

        /// <summary>
        /// Gets the specified keys from the specified object type
        /// </summary>
        public IEnumerable<T> Keys<TKeyTable, T>()
        {
            return new OrmResultSet<T>(this.Context, this.TransformAll(stmt =>
            {
                var innerQuery = stmt.Build();
                var tm = TableMapping.Get(typeof(TKeyTable));

                if (tm.PrimaryKey.Count() != 1)
                    throw new InvalidOperationException(String.Format(ErrorMessages.DATA_STRUCTURE_NOT_APPROPRIATE, nameof(Keys), "nokeys"));

                var sqlParts = this.m_extractRawSelectStatment.Match(innerQuery.SQL);
                if (!sqlParts.Success)
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Keys)));
                }
                else
                {
                    return this.Context.CreateSqlStatement($"SELECT {sqlParts.Groups[SQL_GROUP_DISTINCT].Value} {String.Join(",", tm.PrimaryKey.Select(o => o.Name))} FROM {sqlParts.Groups[SQL_GROUP_FROM].Value} WHERE {sqlParts.Groups[SQL_GROUP_WHERE].Value} {sqlParts.Groups[SQL_GROUP_LIMIT].Value}", innerQuery.Arguments.ToArray());
                }
            }));
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
            sql = sql.Append(" UNION ").Append(other.ToSqlStatement()).Build();
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
            else if (orderExpression is LambdaExpression le &&
                (typeof(CompositeResult).IsAssignableFrom(typeof(TData)) &&
                typeof(TData).GetGenericArguments().Contains(le.Parameters[0].Type) ||
                typeof(TData) == le.Parameters[0].Type)) // This is a composite result - so we want to know if any of the composite objects are TData
            {
                return new OrmResultSet<TData>(this.Context, this.TransformAll(stmt=>stmt.OrderBy(le, Core.Model.Map.SortOrderType.OrderBy)));
            }
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
            else if(orderExpression is LambdaExpression le &&
                (typeof(CompositeResult).IsAssignableFrom(typeof(TData)) &&
                typeof(TData).GetGenericArguments().Contains(le.Parameters[0].Type) ||
                typeof(TData) == le.Parameters[0].Type)) // This is a composite result - so we want to know if any of the composite objects are TData
            {
                return new OrmResultSet<TData>(this.Context, this.TransformAll(stmt => stmt.OrderBy(le, Core.Model.Map.SortOrderType.OrderByDescending)));
            }
            else
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, orderExpression.GetType(), typeof(Expression<Func<TData, dynamic>>)));
        }

        /// <summary>
        /// A non-genericized select statement
        /// </summary>
        public OrmResultSet<TElement> Select<TElement>(String field)
        {
            return new OrmResultSet<TElement>(this.Context, this.TransformAll(stmt =>
           {
               var innerQuery = stmt.Build();
               var sqlParts = this.m_extractRawSelectStatment.Match(innerQuery.SQL);
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
                           return this.Context.CreateSqlStatement($"SELECT {sqlParts.Groups[SQL_GROUP_DISTINCT].Value} {mapping.Table.TableName}.{mapping.Name} FROM {sqlParts.Groups[SQL_GROUP_FROM].Value} WHERE {sqlParts.Groups[SQL_GROUP_WHERE].Value} {sqlParts.Groups[SQL_GROUP_LIMIT].Value}", innerQuery.Arguments.ToArray());
                       }
                   }
                   throw new InvalidOperationException(String.Format(ErrorMessages.FIELD_NOT_FOUND, field));
               }
               else
               {
                   var mapping = TableMapping.Get(typeof(TData)).GetColumn(field);
                   return this.Context.CreateSqlStatement($"SELECT  {sqlParts.Groups[SQL_GROUP_DISTINCT].Value} {mapping.Table.TableName}.{mapping.Name} FROM {sqlParts.Groups[SQL_GROUP_FROM].Value} WHERE {sqlParts.Groups[SQL_GROUP_WHERE].Value} {sqlParts.Groups[SQL_GROUP_LIMIT].Value}", innerQuery.Arguments.ToArray());
               }
           }));
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
            return new OrmResultSet<TData>(this.Context, this.TransformAll(stmt =>
            {
                var sqlParts = this.m_extractRawSelectStatment.Match(stmt.SQL);
                if (!sqlParts.Success)
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(Where)));
                }
                else if (whereExpression is Expression<Func<TData, bool>> whereExpressionStrong)
                {
                    var newStatement = this.Context.CreateSqlStatement($"SELECT {sqlParts.Groups[SQL_GROUP_DISTINCT].Value} ", stmt.Arguments.ToArray());
                    if (typeof(CompositeResult).IsAssignableFrom(typeof(TData)))
                    {
                        newStatement = newStatement.Append(String.Join(",", typeof(TData).GetGenericArguments().SelectMany(t => TableMapping.Get(t).Columns).Select(c => $"{c.Table.TableName}.{c.Name}")));
                    }
                    else
                    {
                        var tmap = TableMapping.Get(typeof(TData));
                        newStatement = newStatement.Append(String.Join(",", tmap.Columns.Select(c => $"{c.Table.TableName}.{c.Name}")));
                    }

                    newStatement.Append($" FROM {sqlParts.Groups[SQL_GROUP_FROM].Value} WHERE ({sqlParts.Groups[SQL_GROUP_WHERE].Value}) ").And(whereExpressionStrong).Append($" {sqlParts.Groups[SQL_GROUP_LIMIT].Value}");
                    return newStatement;
                }
                else
                {
                    throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, typeof(Expression<Func<TData, bool>>), whereExpression.GetType()));
                }
            }));
        }

        /// <summary>
        /// Remove ordering
        /// </summary>
        public IOrmResultSet WithoutOrdering()=>new OrmResultSet<TData>(this.Context, this.TransformAll(stmt => stmt.RemoveOrderBy(out _)));

        /// <summary>
        /// Remove the skip
        /// </summary>
        public IOrmResultSet WithoutSkip(out int originalSkip)
        {
            var skip = 0;
            var retVal = new OrmResultSet<TData>(this.Context, this.TransformAll(stmt=> stmt.RemoveOffset(out skip)));
            originalSkip = skip;
            return retVal;
        }

        /// <summary>
        /// Remove the take instruction
        /// </summary>
        public IOrmResultSet WithoutTake(out int originalTake)
        {
            var take = 0;
            var retVal = new OrmResultSet<TData>(this.Context, this.TransformAll(stmt => stmt.RemoveLimit(out take)));
            originalTake = take;
            return retVal;
        }
    }
}