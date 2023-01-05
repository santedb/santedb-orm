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
using SanteDB.Core.Model.Map;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Represents a SQL statement builder tool
    /// </summary>
    public class SqlStatement
    {
        // Provider
        protected IDbStatementFactory m_statementFactory = null;

        /// <summary>
        /// The SQL statement
        /// </summary>
        private string m_sql = null;

        /// <summary>
        /// RHS of the SQL statement
        /// </summary>
        protected SqlStatement m_rhs = null;

        /// <summary>
        /// Arguments for the SQL statement
        /// </summary>
        protected List<object> m_arguments = null;
        private string m_alias;

        /// <summary>
        /// Get the DB provider
        /// </summary>
        public IDbStatementFactory DbProvider => this.m_statementFactory;

        /// <summary>
        /// Arguments for the SQL Statement
        /// </summary>
        public IEnumerable<Object> Arguments
        {
            get
            {
                return this.m_arguments;
            }
        }

        /// <summary>
        /// Gets the constructed or set SQL
        /// </summary>
        public string SQL
        { 
            get 
            {
                if (this.m_sql?.Contains("--") == true)
                {
                    // Strip out comments
                    return Constants.ExtractCommentsRegex.Replace(this.m_sql ?? "", (m) => m.Groups[1].Value).Replace("\r", " ").Replace("\n", " ");
                }
                else
                {
                    return this.m_sql?.Replace("\r", "").Replace("\n", "") ?? String.Empty;
                }
            } 
        }

        /// <summary>
        /// Gets the alias on the statement
        /// </summary>
        public String Alias => this.m_alias;

        /// <summary>
        /// Creates a new empty SQL statement
        /// </summary>
        public SqlStatement(IDbStatementFactory statementFactory)
        {
            this.m_statementFactory = statementFactory;
        }

        /// <summary>
        /// Create a new sql statement from the specified sql
        /// </summary>
        public SqlStatement(IDbStatementFactory statementFactory, string sql, params object[] parms) : this(statementFactory)
        {
            var sqlParms = this.m_sql?.Count(o => o == '?') ?? 0;
            if (sqlParms > parms.Length)
            {
                throw new ArgumentException($"SQL Statement expects {sqlParms} arguments but only {parms.Length} were supplied");
            }

            this.m_sql = sql;
            this.m_arguments = new List<object>(parms);
        }

        /// <summary>
        /// Append the SQL statement
        /// </summary>
        public SqlStatement Append(SqlStatement sql)
        {
          

            if(sql.m_rhs != null)
            {
                sql = sql.Build();
            }
            else
            {
                sql = new SqlStatement(sql.m_statementFactory, sql.SQL, sql.Arguments.ToArray()); // copy constructr
            }

            if (this.m_rhs != null)
            {
                this.m_rhs.Append(sql);
            }
            else
            {
                this.m_rhs = sql;
            }

            return this;
        }

        /// <summary>
        /// Append the specified SQL
        /// </summary>
        public SqlStatement Append(string sql, params object[] parms)
        {
            return this.Append(new SqlStatement(this.m_statementFactory, sql, parms));
        }

        /// <summary>
        /// Build the special SQL statement
        /// </summary>
        /// <returns></returns>
        public SqlStatement Build()
        {
            StringBuilder sb = new StringBuilder();

            // Parameters
            List<object> parameters = new List<object>();

            var focus = this;
            do
            {
                sb.Append(focus.m_sql);
                // Add parms
                if (focus.Arguments != null)
                {
                    parameters.AddRange(focus.Arguments);
                }

                focus = focus.m_rhs;
            } while (focus != null);

            return new SqlStatement(this.m_statementFactory, sb.ToString(), parameters.ToArray())
            {
                m_alias = this.m_alias
            };
        }

        /// <summary>
        /// Where clause
        /// </summary>
        /// <param name="clause"></param>
        /// <returns></returns>
        public SqlStatement Where(SqlStatement clause)
        {
            if (String.IsNullOrEmpty(clause.SQL) && clause.m_rhs == null)
            {
                return this;
            }
            else if (clause.SQL.Trim().StartsWith("WHERE"))
            {
                return this.Append(clause);
            }
            else
            {
                return this.Append(new SqlStatement(this.m_statementFactory, " WHERE ").Append(clause));
            }
        }

        /// <summary>
        /// Construct a where clause on the expression tree
        /// </summary>
        public SqlStatement Where(String whereClause, params object[] args)
        {
            return this.Where(new SqlStatement(this.m_statementFactory, whereClause, args));
        }

        /// <summary>
        /// Append an AND condition
        /// </summary>
        public SqlStatement And(SqlStatement clause)
        {
            if (String.IsNullOrEmpty(this.m_sql) && (this.m_rhs == null || this.m_rhs.Build().SQL.TrimEnd().EndsWith("where", StringComparison.InvariantCultureIgnoreCase)))
            {
                return this.Append(clause);
            }
            else
            {
                return this.Append(" AND ").Append(clause.Build());
            }
        }

        /// <summary>
        /// Construct a where clause on the expression tree
        /// </summary>
        public SqlStatement And(String clause, params object[] args)
        {
            return this.And(new SqlStatement(this.m_statementFactory, clause, args));
        }

        /// <summary>
        /// Append an AND condition
        /// </summary>
        public SqlStatement Or(SqlStatement clause)
        {
            if (String.IsNullOrEmpty(this.m_sql) && (this.m_rhs == null || this.m_rhs.Build().SQL.TrimEnd().EndsWith("where", StringComparison.InvariantCultureIgnoreCase))
                || this.SQL.TrimEnd().EndsWith("where", StringComparison.InvariantCultureIgnoreCase))
            {
                return this.Append(clause);
            }
            else
            {
                return this.Append(new SqlStatement(this.m_statementFactory, " OR ")).Append(clause.Build());
            }
        }

        /// <summary>
        /// Construct a where clause on the expression tree
        /// </summary>
        public SqlStatement Or(String clause, params object[] args)
        {
            return this.Or(new SqlStatement(this.m_statementFactory, clause, args));
        }

        /// <summary>
        /// Inner join
        /// </summary>
        public SqlStatement InnerJoin<TLeft, TRight>(Expression<Func<TLeft, dynamic>> leftColumn, Expression<Func<TRight, dynamic>> rightColumn)
        {
            return this.Join<TLeft, TRight>("INNER", leftColumn, rightColumn);
        }

        /// <summary>
        /// Join by specific type of join
        /// </summary>
        public SqlStatement Join<TLeft, TRight>(String joinType, Expression<Func<TLeft, dynamic>> leftColumn, Expression<Func<TRight, dynamic>> rightColumn)
        {
            var leftMap = TableMapping.Get(typeof(TLeft));
            var rightMap = TableMapping.Get(typeof(TRight));
            var joinStatement = this.Append($"{joinType} JOIN {rightMap.TableName} ON");
            var rhsPk = rightMap.GetColumn(rightColumn.GetMember());
            var lhsPk = leftMap.GetColumn(leftColumn.GetMember());
            return joinStatement.Append($"({lhsPk.Table.TableName}.{lhsPk.Name} = {rhsPk.Table.TableName}.{rhsPk.Name}) ");
        }

        /// <summary>
        /// Inner join left and right
        /// </summary>
        public SqlStatement InnerJoin(Type tLeft, Type tRight)
        {
            var tableMap = TableMapping.Get(tRight);
            var joinStatement = this.Append($"INNER JOIN {tableMap.TableName} ON ");

            // For RHS we need to find a column which references the tLEFT table ...
            var rhsPk = tableMap.Columns.SingleOrDefault(o => o.ForeignKey?.Table == tLeft);
            ColumnMapping lhsPk = null;
            if (rhsPk == null) // look for primary key instead
            {
                rhsPk = tableMap.Columns.SingleOrDefault(o => o.IsPrimaryKey);
                lhsPk = TableMapping.Get(tLeft).Columns.SingleOrDefault(o => o.ForeignKey?.Table == rhsPk.Table.OrmType && o.ForeignKey?.Column == rhsPk.SourceProperty.Name);
            }
            else
            {
                lhsPk = TableMapping.Get(rhsPk.ForeignKey.Table).GetColumn(rhsPk.ForeignKey.Column);
            }

            if (lhsPk == null || rhsPk == null) // Try a natural join
            {
                rhsPk = tableMap.Columns.SingleOrDefault(o => TableMapping.Get(tLeft).Columns.Any(l => o.Name == l.Name));
                lhsPk = TableMapping.Get(tLeft).Columns.SingleOrDefault(o => o.Name == rhsPk?.Name);
                if (rhsPk == null || lhsPk == null)
                {
                    throw new InvalidOperationException("Unambiguous linked keys not found");
                }
            }
            joinStatement.Append($"({lhsPk.Table.TableName}.{lhsPk.Name} = {rhsPk.Table.TableName}.{rhsPk.Name}) ");
            return joinStatement;
        }

        /// <summary>
        /// Return a select from
        /// </summary>
        public SqlStatement SelectFrom(Type dataType)
        {
            var tableMap = TableMapping.Get(dataType);
            return this.Append(new SqlStatement(this.m_statementFactory, $"SELECT * FROM {tableMap.TableName} AS {tableMap.TableName} "));
        }

        /// <summary>
        /// Return a delete from
        /// </summary>
        public SqlStatement DeleteFrom(Type dataType)
        {
            var tableMap = TableMapping.Get(dataType);
            return this.Append(new SqlStatement(this.m_statementFactory, $"DELETE FROM {tableMap.TableName} "));
        }

        /// <summary>
        /// Return a select from
        /// </summary>
        public SqlStatement SelectFrom(Type dataType, params ColumnMapping[] columns)
        {
            var tableMap = TableMapping.Get(dataType);
            return this.Append(new SqlStatement(this.m_statementFactory, $"SELECT {String.Join(",", columns.Select(o => $"{this.m_alias ?? o.Table.TableName}.{o.Name}"))} FROM {tableMap.TableName} AS {tableMap.TableName} "));
        }

        /// <summary>
        /// Construct a where clause on the expression tree
        /// </summary>
        public SqlStatement Where<TExpression>(Expression<Func<TExpression, bool>> expression)
        {
            var tableMap = TableMapping.Get(typeof(TExpression));
            var queryBuilder = new SqlQueryExpressionBuilder(this.Alias, this.m_statementFactory);
            queryBuilder.Visit(expression.Body);
            return this.Append(new SqlStatement(this.m_statementFactory, "WHERE ").Append(queryBuilder.SqlStatement.Build()));
        }


        /// <summary>
        /// Expression
        /// </summary>
        public SqlStatement And<TExpression>(Expression<Func<TExpression, bool>> expression)
        {
            var tableMap = TableMapping.Get(typeof(TExpression));
            var queryBuilder = new SqlQueryExpressionBuilder(tableMap.TableName, this.m_statementFactory);
            queryBuilder.Visit(expression.Body);
            return this.And(queryBuilder.SqlStatement.Build());
        }

        /// <summary>
        /// Append an offset statement
        /// </summary>
        public SqlStatement Offset(int offset)
        {
            if (this.m_statementFactory.Features.HasFlag(SqlEngineFeatures.LimitOffset))
            {
                // Need a limit before this statement
                return this.RemoveOffset(out _).Append($" OFFSET {offset} ");
            }
            else if (this.m_statementFactory.Features.HasFlag(SqlEngineFeatures.FetchOffset))
            {
                return this.RemoveOffset(out _).Append($" OFFSET {offset} ROW ");
            }
            else
            {
                throw new InvalidOperationException("SQL Engine does not support OFFSET n ROW or OFFSET n");
            }
        }

        /// <summary>
        /// Remove the offset instruction
        /// </summary>
        public SqlStatement RemoveOffset(out int offset)
        {
            var sql = this.Build();
            var sqlPart = Constants.ExtractOffsetRegex.Match(sql.SQL);
            if (sqlPart.Success)
            {
                offset = Int32.Parse(sqlPart.Groups[1].Value);
                return new SqlStatement(this.m_statementFactory, $"{sql.SQL.Substring(0, sqlPart.Index)} {sql.SQL.Substring(sqlPart.Index + sqlPart.Length)}", sql.Arguments.ToArray());
            }
            offset = 0;
            return sql;
        }

        /// <summary>
        /// Limit of the
        /// </summary>
        public SqlStatement Limit(int limit)
        {
            if (this.m_statementFactory.Features.HasFlag(SqlEngineFeatures.LimitOffset))
            {

                return this.RemoveLimit(out _).Append($" LIMIT {limit} ")  ;
            }
            else if (this.m_statementFactory.Features.HasFlag(SqlEngineFeatures.FetchOffset))
            {
                return this.RemoveLimit(out _).Append($" FETCH FIRST {limit} ROWS ONLY");
            }
            else
            {
                throw new InvalidOperationException("SQL Engine does not support FETCH FIRST n ROWS ONLY or LIMIT n functionality");
            }
        }

        /// <summary>
        /// Remove the limit instruction
        /// </summary>
        public SqlStatement RemoveLimit(out int count)
        {
            var sql = this.Build();
            var sqlPart = Constants.ExtractLimitRegex.Match(sql.SQL + " ");
            if (sqlPart.Success)
            {
                count = Int32.Parse(sqlPart.Groups[1].Value);
                return new SqlStatement(this.m_statementFactory, $"{sql.SQL.Substring(0, sqlPart.Index)} {sql.SQL.Substring(sqlPart.Index + sqlPart.Length)}", sql.Arguments.ToArray());
            }
            count = -1;
            return sql;
        }

        /// <summary>
        /// Construct an order by
        /// </summary>
        public SqlStatement OrderBy<TData>(Expression<Func<TData, dynamic>> orderExpression, SortOrderType sortOperation = SortOrderType.OrderBy) => this.OrderBy((LambdaExpression)orderExpression, sortOperation);

        /// <summary>
        /// Using an alias for column references
        /// </summary>
        /// <param name="alias">The alias to use</param>
        public SqlStatement UsingAlias(String alias)
        {
            this.m_alias = alias;
            return this;
        }

        /// <summary>
        /// Construct an order by
        /// </summary>
        public SqlStatement OrderBy(LambdaExpression orderExpression, SortOrderType sortOperation = SortOrderType.OrderBy)
        {
            var orderMap = TableMapping.Get(orderExpression.Parameters[0].Type);
            var fldRef = orderExpression.Body;
            while (fldRef.NodeType != ExpressionType.MemberAccess)
            {
                switch (fldRef.NodeType)
                {
                    case ExpressionType.Convert:
                        fldRef = (fldRef as UnaryExpression).Operand;
                        break;

                    case ExpressionType.Call:
                        fldRef = (fldRef as MethodCallExpression).Object;
                        break;
                }
            }
            var orderCol = orderMap.GetColumn(fldRef.GetMember());

            // Is there already an orderby in the previous statement?
            bool hasOrder = false;
            var t = this;
            do
            {
                hasOrder |= t.SQL?.Contains("ORDER BY") == true;
                t = t.m_rhs;
            } while (t != null);

            // Append order by?
            return this.Append($"{(!hasOrder ? " ORDER BY " : ",")} {this.m_alias ?? orderCol.Table.TableName}.{orderCol.Name} {(sortOperation == SortOrderType.OrderBy ? " ASC " : " DESC ")}");
        }

        /// <summary>
        /// Get the last statement
        /// </summary>
        public SqlStatement GetLast()
        {
            return this.GetSecondLast().m_rhs;
        }

        /// <summary>
        /// Get second last
        /// </summary>
        private SqlStatement GetSecondLast()
        { 
            var t = this;
            while (t.m_rhs?.m_rhs != null)
            {
                t = t.m_rhs;
            }

            return t;
        }

        /// <summary>
        /// Remove the ORDER BY clause
        /// </summary>
        public SqlStatement RemoveOrderBy(out SqlStatement orderBy)
        {
            var sql = this.Build();
            var sqlPart = Constants.ExtractOrderByRegex.Match(sql.SQL);
            if (sqlPart.Success)
            {
                orderBy = new SqlStatement(this.m_statementFactory, sqlPart.Groups[2].Value);
                return new SqlStatement(this.m_statementFactory, $"{sqlPart.Groups[1].Value} {sqlPart.Groups[3].Value}", sql.Arguments.ToArray());
            }
            orderBy = null;
            return this;

        }

        /// <summary>
        /// Removes the last statement from the list
        /// </summary>
        public SqlStatement RemoveLast(out SqlStatement lastStatement)
        {
            var t = this.GetSecondLast();
            if (t != null)
            {
                var m = t.m_rhs;
                t.m_rhs = null;
                lastStatement = m;
                return this;
            }
            else
            {
                lastStatement = null;
                return this;
            }
        }

        /// <summary>
        /// Represent as string
        /// </summary>
        public override string ToString()
        {
            return this.Build().SQL;
        }

        /// <summary>
        /// Generate an update statement
        /// </summary>
        public SqlStatement UpdateSet(Type tableType)
        {
            var tableMap = TableMapping.Get(tableType);
            return this.Append(new SqlStatement(this.m_statementFactory, $"UPDATE {tableMap.TableName} SET "));
        }
    }

    /// <summary>
    /// Represents a strongly typed SQL Statement
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SqlStatement<T> : SqlStatement
    {
        // The alias of the table if needed
        private String m_alias;

        /// <summary>
        /// Gets the table type
        /// </summary>
        public Type TableType
        { get { return typeof(T); } }

        /// <summary>
        /// Creates a new empty SQL statement
        /// </summary>
        public SqlStatement(IDbStatementFactory factory) : base(factory)
        {
        }

        /// <summary>
        /// Create a new sql statement from the specified sql
        /// </summary>
        public SqlStatement(IDbStatementFactory factory, string sql, params object[] parms) : base(factory, sql, parms)
        {
        }

        /// <summary>
        /// Append the SQL statement
        /// </summary>
        public SqlStatement<T> Append(SqlStatement<T> sql)
        {
            
            if (this.m_rhs != null)
            {
                this.m_rhs.Append(sql);
            }
            else
            {
                this.m_rhs = sql;
            }

            return this;
        }

        /// <summary>
        /// Inner join
        /// </summary>
        public SqlStatement<T> InnerJoin<TRight>(Expression<Func<T, dynamic>> leftColumn, Expression<Func<TRight, dynamic>> rightColumn)
        {
            var leftMap = TableMapping.Get(typeof(T));
            var rightMap = TableMapping.Get(typeof(TRight));
            var joinStatement = this.Append($"INNER JOIN {rightMap.TableName} ON ");
            var rhsPk = rightMap.GetColumn(rightColumn.Body.GetMember());
            var lhsPk = leftMap.GetColumn(leftColumn.Body.GetMember());
            var retVal = new SqlStatement<T>(this.m_statementFactory);
            retVal.Append(joinStatement).Append($"({lhsPk.Table.TableName}.{lhsPk.Name} = {rhsPk.Table.TableName}.{rhsPk.Name}) ");
            return retVal;
        }

        /// <summary>
        /// Construct a where clause on the expression tree
        /// </summary>
        public SqlStatement Where(Expression<Func<T, bool>> expression)
        {
            var tableMap = TableMapping.Get(typeof(T));
            var queryBuilder = new SqlQueryExpressionBuilder(this.m_alias ?? tableMap.TableName, this.m_statementFactory);
            queryBuilder.Visit(expression.Body);
            return this.Append(new SqlStatement(this.m_statementFactory, "WHERE ").Append(queryBuilder.SqlStatement));
        }

        /// <summary>
        /// Appends an inner join
        /// </summary>
        public SqlStatement<TReturn> AutoJoin<TJoinTable, TReturn>()
        {
            var retVal = new SqlStatement<TReturn>(this.m_statementFactory);
            retVal.Append(this).InnerJoin(typeof(T), typeof(TJoinTable));
            return retVal;
        }

        /// <summary>
        /// Create a delete from
        /// </summary>
        public SqlStatement<T> DeleteFrom()
        {
            var tableMap = TableMapping.Get(typeof(T));
            return this.Append(new SqlStatement<T>(this.m_statementFactory, $"DELETE FROM {tableMap.TableName} "));
        }

        /// <summary>
        /// Construct a SELECT FROM statement
        /// </summary>
        public SqlStatement<T> SelectFrom()
        {
            var tableMap = TableMapping.Get(typeof(T));
            SqlStatement<T> retVal = new SqlStatement<T>(this.m_statementFactory, "SELECT ");
            if (this.m_statementFactory.Features.HasFlag(SqlEngineFeatures.StrictSubQueryColumnNames))
            {
                retVal.Append(new SqlStatement<T>(this.m_statementFactory, $"{String.Join(",", tableMap.Columns.Select(c => $"{tableMap.TableName}.{c.Name}").ToArray())}")
                {
                    m_alias = tableMap.TableName
                });
            }
            else
            {
                retVal.Append(new SqlStatement<T>(this.m_statementFactory, $"*")
                {
                    m_alias = tableMap.TableName
                });
            }

            retVal.Append(new SqlStatement<T>(this.m_statementFactory, $" FROM {tableMap.TableName} AS {tableMap.TableName} "));
            return retVal;
        }

        /// <summary>
        /// Construct a SELECT FROM statement with the specified selectors
        /// </summary>
        /// <param name="columns">The columns from which to select data</param>
        /// <returns>The constructed sql statement</returns>
        public SqlStatement<T> SelectFrom(params ColumnMapping[] columns)
        {
            var tableMap = TableMapping.Get(typeof(T));
            return this.Append(new SqlStatement<T>(this.m_statementFactory, $"SELECT {String.Join(",", columns.Select(o => o.Table == null ? o.Name : $"{this.m_alias ?? o.Table.TableName}.{o.Name}"))} FROM {tableMap.TableName} AS {tableMap.TableName} "));
        }

        /// <summary>
        /// Construct a SELECT FROM statement with the specified selectors
        /// </summary>
        /// <param name="scopedTables">The types from which to select columns</param>
        /// <returns>The constructed sql statement</returns>
        public SqlStatement<T> SelectFrom(params Type[] scopedTables)
        {
            var existingCols = new List<String>();
            var tableMap = TableMapping.Get(typeof(T));
            // Column list of distinct columns
            var columnList = String.Join(",", scopedTables.Select(o => TableMapping.Get(o)).SelectMany(o => o.Columns).Where(o =>
              {
                  if (!existingCols.Contains(o.Name))
                  {
                      existingCols.Add(o.Name);
                      return true;
                  }
                  return false;
              }).Select(o => $"{this.m_alias ?? (!o.Table.HasName ? "" : o.Table.TableName + ".")}{o.Name}"));

            // Append the result to query
            var retVal = this.Append(new SqlStatement<T>(this.m_statementFactory, $"SELECT {columnList} ")
            {
                m_alias = tableMap.TableName
            });

            retVal.Append($" FROM {tableMap.TableName} AS {tableMap.TableName} ");

            return retVal;
        }

        /// <summary>
        /// Generate an update statement
        /// </summary>
        public SqlStatement<T> UpdateSet()
        {
            var tableMap = TableMapping.Get(typeof(T));
            return this.Append(new SqlStatement<T>(this.m_statementFactory, $"UPDATE {tableMap.TableName} SET "));
        }
    }
}