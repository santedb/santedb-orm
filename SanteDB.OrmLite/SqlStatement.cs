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
using SanteDB.Core.i18n;
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
    /// Represents a finalized SqlStatement
    /// </summary>
    /// TODO: Figure out how to make this a readonly struct?
    public sealed class SqlStatement
    {
        private readonly string m_sql;
        private readonly object[] m_arguments;
        private readonly string m_alias;
        private SqlStatement m_next;
        private readonly bool m_isPrepared;

        /// <summary>
        /// Get an empty SQL statement
        /// </summary>
        public static readonly SqlStatement Empty = new SqlStatement(String.Empty);

        /// <summary>
        /// True if this statement has been prepared 
        /// </summary>
        public bool IsPrepared => this.m_isPrepared;

        /// <summary>
        /// Gets the SQL 
        /// </summary>
        public String Sql => this.m_sql;

        /// <summary>
        /// Gets the arguments
        /// </summary>
        public Object[] Arguments => this.m_arguments;

        /// <summary>
        /// Gets the alias
        /// </summary>
        public String Alias => this.m_alias;

        /// <summary>
        /// Create a new SQL statement
        /// </summary>
        private SqlStatement(String alias, String sql, SqlStatement next, bool isPrepared, params object[] arguments)
        {
            sql = sql ?? String.Empty;
            if (sql.Contains("--") == true)
            {
                // Strip out comments
                this.m_sql = Constants.ExtractCommentsRegex.Replace(sql ?? "", (m) => m.Groups[1].Value).Replace("\r", " ").Replace("\n", " ");
            }
            else if (sql.Contains("\n"))
            {
                this.m_sql = sql?.Replace("\r", "").Replace("\n", "");
            }
            else
            {
                this.m_sql = sql;
            }

            this.m_isPrepared = isPrepared;
            this.m_alias = alias;
            this.m_arguments = arguments ?? new object[0];
            this.m_next = next;
        }

        /// <summary>
        /// Create a new SqlStatmeent
        /// </summary>
        public SqlStatement(String sql, params object[] arguments) : this(null, sql, null, false, arguments) { }

        /// <summary>
        /// Create new SQL statement
        /// </summary>
        public SqlStatement(String alias, String sql, params object[] arguments) : this(alias, sql, null, false, arguments)
        {
            var expected = sql.Count(c => c == '?');
            if (expected != arguments.Length)
            {
                throw new ArgumentOutOfRangeException(String.Format(ErrorMessages.ARGUMENT_COUNT_MISMATCH, expected, arguments.Length));
            }
        }

        /// <summary>
        /// Build a sql statement from a copy
        /// </summary>
        public SqlStatement(SqlStatement copyFrom, String alias = null) : this(alias ?? copyFrom.m_alias, copyFrom.m_sql, copyFrom.m_next?.Copy(), false, copyFrom.m_arguments) { }

        /// <summary>
        /// Copy this object
        /// </summary>
        private SqlStatement Copy()
        {
            return new SqlStatement(this);
        }

        /// <summary>
        /// Collapses this SqlStatement into a single statement for execution
        /// </summary>
        /// <returns></returns>
        public SqlStatement Prepare()
        {
            if (this.m_isPrepared)
            {
                return this;
            }

            StringBuilder sb = new StringBuilder();

            // Parameters
            LinkedList<object> parameters = new LinkedList<object>();

            SqlStatement focus = this;
            while (focus != null)
            {
                sb.Append(focus.m_sql);
                // Add parms
                foreach (var itm in focus.m_arguments)
                {
                    parameters.AddLast(itm);
                }

                focus = focus.m_next;
            }

            return new SqlStatement(this.m_alias, sb.ToString(), null, true, parameters.ToArray());
        }

        /// <summary>
        /// True if the statement is empty
        /// </summary>
        public bool IsEmpty() => String.IsNullOrEmpty(this.m_sql.Trim()) && this.m_arguments.Length == 0 && (this.m_next == null || this.m_next.IsEmpty());

        /// <summary>
        /// True if any of the components in this statement end with <paramref name="partialStatement"/>
        /// </summary>
        /// <param name="partialStatement">The partial statement</param>
        /// <param name="stringComparison">The comparison mode</param>
        /// <returns>True if any part of this statement ends with</returns>
        public bool EndsWith(String partialStatement, StringComparison stringComparison)
        {
            return this.Last().Sql.TrimEnd().EndsWith(partialStatement, stringComparison);
        }

        /// <summary>
        /// True if this sql statement contains <paramref name="partialStatement"/>
        /// </summary>
        /// <param name="partialStatement">The statement keyword to search</param>
        public bool Contains(String partialStatement)
        {
            var search = this;
            while (true)
            {
                if (search.m_sql.Contains(partialStatement))
                {
                    return true;
                }
                if (search.m_next != null)
                {
                    search = (SqlStatement)search.m_next;
                }
                else
                {
                    break;
                }
            }
            return false;
        }

        /// <summary>
        /// Concatenate the two sql statements together
        /// </summary>
        public static SqlStatement operator +(SqlStatement a, SqlStatement b)
        {
            return a.Append(b);
        }

        /// <summary>
        /// Concatenate with a simple string
        /// </summary>
        public static SqlStatement operator +(SqlStatement a, string b)
        {
            return a.Append(b);
        }

        /// <summary>
        /// Concatenate with a simple string
        /// </summary>
        public static SqlStatement operator +(string a, SqlStatement b)
        {
            return new SqlStatement(null, a, b, false);
        }

        /// <summary>
        /// Append sql statement <paramref name="other"/> to this and return a new SqlStatement
        /// </summary>
        public SqlStatement Append(SqlStatement other)
        {
            if (other == null) return this;

            var retVal = new SqlStatement(this);
            var focus = retVal;
            retVal.Last().m_next = new SqlStatement(other);
            return retVal;
        }

        /// <summary>
        /// Append sql statement <paramref name="other"/> to this and return a new SqlStatement
        /// </summary>
        public SqlStatement Append(String other)
        {
            return this.Append(new SqlStatement(other));
        }

        /// <summary>
        /// Remove the first component matching the specified <paramref name="pattern"/>
        /// </summary>
        internal SqlStatement RemoveMatching(Regex pattern, out SqlStatement removed)
        {
            var retVal = new SqlStatement(this);
            var focus = retVal;
            removed = null;
            while (focus?.m_next != null)
            {
                if (pattern.IsMatch(focus.m_next.Sql))
                {
                    removed = focus.m_next;
                    focus.m_next = focus.m_next.m_next; // skip this node in copy tree
                    focus = focus.m_next;
                }
                focus = focus?.m_next;
            }
            return retVal;
        }

        /// <summary>
        /// Get the last in the tree
        /// </summary>
        public SqlStatement Last()
        {
            var focal = this;
            while (focal.m_next != null)
            {
                focal = focal.m_next;
            }
            return focal;
        }

        /// <summary>
        /// Remove the last statement from this SqlStatement
        /// </summary>
        public SqlStatement RemoveLast(out SqlStatement lastStatement)
        {
            var retVal = new SqlStatement(this);
            var focal = retVal;
            while(focal.m_next?.m_next != null)
            {
                focal = focal.m_next;
            }
            lastStatement = focal.m_next;
            focal.m_next = null;
            return retVal;
             
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            var focus = this;
            while (focus != null)
            {
                sb.Append(focus.m_sql);
                focus = focus.m_next;
            }
            return sb.ToString();
        }

        /// <summary>
        /// Represent as a literal query string
        /// </summary>
        public string ToLiteral()
        {
            var query = this.Prepare();
            StringBuilder retVal = new StringBuilder(query.ToString());
            String sql = retVal.ToString();
            int parmId = 0;
            int lastIndex = 0;
            while (sql.IndexOf("?", lastIndex) > -1)
            {
                var pIndex = sql.IndexOf("?", lastIndex);
                retVal.Remove(pIndex, 1);
                var obj = query.m_arguments[parmId++];
                if (obj is String || obj is Guid || obj is Guid? || obj is DateTime || obj is DateTimeOffset)
                {
                    obj = $"'{obj}'";
                }
                else if (obj == null)
                {
                    obj = "null";
                }

                retVal.Insert(pIndex, obj);
                sql = retVal.ToString();
                lastIndex = pIndex + obj.ToString().Length;
            }
            return retVal.ToString();
        }

        /// <summary>
        /// Reduces any empty statements from this chain
        /// </summary>
        /// <returns></returns>
        public SqlStatement Reduce()
        {
            if(this.m_next == null)
            {
                return this;
            }

            SqlStatement retVal = null, readFocal = this, workFocal = null;
            while(readFocal != null)
            {
                if(!String.IsNullOrEmpty(readFocal.Sql))
                {
                    if(retVal != null)
                    {
                        workFocal = workFocal.m_next = new SqlStatement(readFocal);
                    }
                    else
                    {
                        workFocal = retVal = new SqlStatement(readFocal);
                    }
                }
                else if(workFocal != null)
                {
                    workFocal.m_next = null;
                }
                readFocal = readFocal.m_next;
            }
            return retVal;
        }
    }

    /// <summary>
    /// Represents a SQL statement builder tool
    /// </summary>
    public class SqlStatementBuilder
    {
        // Provider
        private readonly IDbStatementFactory m_statementFactory = null;

        /// <summary>
        /// The SQL statement
        /// </summary>
        private SqlStatement m_sqlStatement = SqlStatement.Empty;

        /// <summary>
        /// Get the DB provider
        /// </summary>
        public IDbStatementFactory DbProvider => this.m_statementFactory;

        /// <summary>
        /// Gets the constructed or set SQL
        /// </summary>
        public SqlStatement Statement => this.m_sqlStatement;

        /// <summary>
        /// Creates a new empty SQL statement
        /// </summary>
        public SqlStatementBuilder(IDbStatementFactory statementFactory, SqlStatement statement = null)
        {
            this.m_statementFactory = statementFactory;
            this.m_sqlStatement = statement?.Reduce() ?? SqlStatement.Empty;

        }

        /// <summary>
        /// Create a new sql statement from the specified sql
        /// </summary>
        public SqlStatementBuilder(IDbStatementFactory statementFactory, string sql, params object[] parms) : this(statementFactory)
        {
            this.m_sqlStatement = new SqlStatement(sql, parms);
        }

        /// <summary>
        /// Construct a SELECT FROM statement with the specified selectors
        /// </summary>
        /// <param name="scopedTables">The types from which to select columns</param>
        /// <returns>The constructed sql statement</returns>
        public SqlStatementBuilder SelectFrom(params Type[] scopedTables)
        {
            if(scopedTables.Length == 0)
            {
                throw new ArgumentException(String.Format(ErrorMessages.ARGUMENT_COUNT_MISMATCH, 1, 0), nameof(scopedTables));
            }
            var existingCols = new List<String>();
            var tableMap = TableMapping.Get(scopedTables[0]);
            // Column list of distinct columns
            var columnList = String.Join(",", scopedTables.Select(o => TableMapping.Get(o)).SelectMany(o => o.Columns).Where(o =>
            {
                if (!existingCols.Contains(o.Name))
                {
                    existingCols.Add(o.Name);
                    return true;
                }
                return false;
            }).Select(o => $"{(!o.Table.HasName ? "" : o.Table.TableName + ".")}{o.Name}"));

            // Append the result to query
            this.Append($"SELECT {columnList} FROM {tableMap.TableName} AS {tableMap.TableName}");
            return this;
        }

        /// <summary>
        /// Wrap as a subquery
        /// </summary>
        public SqlStatementBuilder WrapAsSubQuery(params ColumnMapping[] columns)
        {
            var alias = this.IncrementSubqueryAlias();
            this.m_sqlStatement = new SqlStatement(alias, $"SELECT {String.Join(",", columns.Select(o => $"{alias}.{o.Name}").Distinct())} FROM (") +
                this.m_sqlStatement + $") AS {alias}";
            return this;
        }

        /// <summary>
        /// Wrap as a subquery
        /// </summary>
        public SqlStatementBuilder WrapAsSubQuery()
        {
            var alias = this.IncrementSubqueryAlias();
            var sqlParts = Constants.ExtractRawSqlStatementRegex.Match(this.Statement.ToString());
            if (sqlParts.Success)
            {
                this.m_sqlStatement = new SqlStatement(alias, $"SELECT {sqlParts.Groups[Constants.SQL_GROUP_DISTINCT].Value} {this.RebindColumnSelector(sqlParts.Groups[Constants.SQL_GROUP_COLUMNS].Value, alias)} FROM (") +
                    this.m_sqlStatement + $") AS {alias}";
                return this;
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(WrapAsSubQuery)));
            }
        }

        /// <summary>
        /// Increment the sub-query alias
        /// </summary>
        private string IncrementSubqueryAlias()
        {
            if(String.IsNullOrEmpty(this.m_sqlStatement.Alias))
            {
                return "SA0";
            }
            else if(Int32.TryParse(this.m_sqlStatement.Alias.Substring(2), out var sqi))
            {
                return $"SQ{++sqi}";
            }
            else
            {
                return "SQ9";
            }
        }

        /// <summary>
        /// Re-binds columns from a select statement to <paramref name="newTableAlias"/>
        /// </summary>
        /// <param name="columnSelector">The column selector example <c>SELECT table.col, table.col2, col3</c> becomes <c>SELECT newTable.col, newTable.col2, newTable.col3</c></param>
        /// <param name="newTableAlias">The new table alias</param>
        private string RebindColumnSelector(string columnSelector, string newTableAlias)
        {
            var matchedColumns = Constants.ExtractColumnBindingRegex.Matches(columnSelector).OfType<Match>().Select(o => $"{newTableAlias}.{o.Groups[2].Value}").Distinct();
            return String.Join(",", matchedColumns);
        }

        /// <summary>
        /// Append the SQL statement
        /// </summary>
        public SqlStatementBuilder Append(SqlStatement sql)
        {
            this.m_sqlStatement += sql;
            return this;
        }


        /// <summary>
        /// Append all the commands in <paramref name="otherBuilder"/>
        /// </summary>
        public SqlStatementBuilder Append(SqlStatementBuilder otherBuilder)
        {
            this.m_sqlStatement += otherBuilder.m_sqlStatement;
            return this;
        }

        /// <summary>
        /// Append the specified SQL
        /// </summary>
        public SqlStatementBuilder Append(string sql, params object[] parms)
        {
            this.m_sqlStatement += new SqlStatement(sql, parms);
            return this;
        }

        /// <summary>
        /// Where clause
        /// </summary>
        public SqlStatementBuilder Where(SqlStatement clause)
        {
            if (clause.IsEmpty())
            {
                return this;
            }
            else if (clause.Sql.StartsWith("WHERE", StringComparison.OrdinalIgnoreCase))
            {
                this.m_sqlStatement += clause;
            }
            else
            {
                this.m_sqlStatement += " WHERE " + clause;
            }
            return this;
        }

        /// <summary>
        /// Construct a where clause on the expression tree
        /// </summary>
        public SqlStatementBuilder Where(String whereClause, params object[] args)
        {
            return this.Where(new SqlStatement(whereClause, args));
        }

        /// <summary>
        /// Append an AND condition
        /// </summary>
        public SqlStatementBuilder And(SqlStatement clause)
        {
            if (this.m_sqlStatement.IsEmpty() || this.m_sqlStatement.EndsWith("where", StringComparison.CurrentCultureIgnoreCase)
                || this.m_sqlStatement.ToString().TrimEnd().EndsWith("and", StringComparison.CurrentCultureIgnoreCase))// || clause.AnyEndsWith("where", StringComparison.OrdinalIgnoreCase))
            {
                this.m_sqlStatement += clause;
            }
            else
            {
                this.m_sqlStatement += " AND " + clause;
            }
            return this;
        }

        /// <summary>
        /// Construct a where clause on the expression tree
        /// </summary>
        public SqlStatementBuilder And(String clause, params object[] args)
        {
            return this.And(new SqlStatement(clause, args));
        }

        /// <summary>
        /// Append an AND condition
        /// </summary>
        public SqlStatementBuilder Or(SqlStatement clause)
        {
            if (this.m_sqlStatement.IsEmpty() || this.m_sqlStatement.EndsWith("where", StringComparison.CurrentCultureIgnoreCase)
               || this.m_sqlStatement.ToString().TrimEnd().EndsWith("or", StringComparison.CurrentCultureIgnoreCase))// || clause.AnyEndsWith("where", StringComparison.OrdinalIgnoreCase))
            {
                this.m_sqlStatement += clause;
            }
            else
            {
                this.m_sqlStatement += " OR " + clause;
            }
            return this;
        }

        /// <summary>
        /// Construct a where clause on the expression tree
        /// </summary>
        public SqlStatementBuilder Or(String clause, params object[] args)
        {
            return this.Or(new SqlStatement(clause, args));
        }

        /// <summary>
        /// Inner join
        /// </summary>
        public SqlStatementBuilder InnerJoin<TLeft, TRight>(Expression<Func<TLeft, dynamic>> leftColumn, Expression<Func<TRight, dynamic>> rightColumn)
        {
            return this.Join<TLeft, TRight>("INNER", leftColumn, rightColumn);
        }

        /// <summary>
        /// Join by specific type of join
        /// </summary>
        public SqlStatementBuilder Join<TLeft, TRight>(String joinType, Expression<Func<TLeft, dynamic>> leftColumn, Expression<Func<TRight, dynamic>> rightColumn)
        {
            var leftMap = TableMapping.Get(typeof(TLeft));
            var rightMap = TableMapping.Get(typeof(TRight));
            this.m_sqlStatement += $" {joinType} JOIN {rightMap.TableName} ON ";
            var rhsPk = rightMap.GetColumn(rightColumn.GetMember());
            var lhsPk = leftMap.GetColumn(leftColumn.GetMember());
            this.m_sqlStatement += $" ({lhsPk.Table.TableName}.{lhsPk.Name} = {rhsPk.Table.TableName}.{rhsPk.Name}) ";
            return this;
        }


        /// <summary>
        /// Return a delete from
        /// </summary>
        public SqlStatementBuilder DeleteFrom(Type dataType)
        {
            var tableMap = TableMapping.Get(dataType);
            this.m_sqlStatement += $"DELETE FROM {tableMap.TableName} ";
            return this;
        }

        /// <summary>
        /// Return a select from
        /// </summary>
        public SqlStatementBuilder SelectFrom(Type dataType, params ColumnMapping[] columns)
        {
            if (columns.Length == 0)
            {
                return this.SelectFrom(new Type[] { dataType });
            }
            else
            {
                var tableMap = TableMapping.Get(dataType);
                var colnames = String.Join(",", columns.Select(o =>
                {
                    if (o.Table != null)
                    {
                        return $"{this.m_sqlStatement.Alias ?? o.Table.TableName}.{o.Name}";
                    }
                    else
                    {
                        return o.Name;
                    }
                }));
                this.m_sqlStatement += $"SELECT {colnames} FROM {tableMap.TableName} AS {tableMap.TableName} ";
                return this;
            }
        }

        /// <summary>
        /// Construct a where clause on the expression tree
        /// </summary>
        public SqlStatementBuilder Where<TExpression>(Expression<Func<TExpression, bool>> expression) => this.Where((LambdaExpression)expression);

        /// <summary>
        /// Construct a where clause
        /// </summary>
        public SqlStatementBuilder Where(LambdaExpression lambdaExpression)
        {
            var tableMap = TableMapping.Get(lambdaExpression.Parameters[0].Type);
            var queryBuilder = new SqlQueryExpressionBuilder(this.m_sqlStatement.Alias, this.m_statementFactory);
            queryBuilder.Visit(lambdaExpression.Body);
            this.m_sqlStatement += " WHERE " + queryBuilder.StatementBuilder.Statement;
            return this;
        }


        /// <summary>
        /// Expression
        /// </summary>
        public SqlStatementBuilder And<TExpression>(Expression<Func<TExpression, bool>> expression)
        {
            var tableMap = TableMapping.Get(typeof(TExpression));
            var queryBuilder = new SqlQueryExpressionBuilder(tableMap.TableName, this.m_statementFactory);
            queryBuilder.Visit(expression.Body);
            this.And(queryBuilder.StatementBuilder.Statement);
            return this;
        }

        /// <summary>
        /// Append an offset statement
        /// </summary>
        public SqlStatementBuilder Offset(int offset)
        {
            this.RemoveOffset(out _);
            if (this.m_statementFactory.Features.HasFlag(SqlEngineFeatures.LimitOffset))
            {
                // Need a limit before this statement
                this.m_sqlStatement += $" OFFSET {offset} ";
            }
            else if (this.m_statementFactory.Features.HasFlag(SqlEngineFeatures.FetchOffset))
            {
                this.m_sqlStatement += $" OFFSET {offset} ROW ";
            }
            else
            {
                throw new InvalidOperationException("SQL Engine does not support OFFSET n ROW or OFFSET n");
            }
            return this;
        }

        /// <summary>
        /// Remove the offset instruction
        /// </summary>
        public SqlStatementBuilder RemoveOffset(out int offset)
        {
            this.m_sqlStatement = this.m_sqlStatement.RemoveMatching(Constants.ExtractOffsetRegex, out var removed);
            if (removed != null)
            {
                offset = Int32.Parse(Constants.ExtractOffsetRegex.Match(removed.Sql).Groups[1].Value);
            }
            else
            {
                offset = 0;
            }
            return this;
        }

        /// <summary>
        /// Limit of the
        /// </summary>
        public SqlStatementBuilder Limit(int limit)
        {
            this.RemoveLimit(out _);

            if (this.m_statementFactory.Features.HasFlag(SqlEngineFeatures.LimitOffset))
            {
                this.m_sqlStatement += $" LIMIT {limit} ";
            }
            else if (this.m_statementFactory.Features.HasFlag(SqlEngineFeatures.FetchOffset))
            {
                this.m_sqlStatement += $" FETCH FIRST {limit} ROWS ONLY";
            }
            else
            {
                throw new InvalidOperationException("SQL Engine does not support FETCH FIRST n ROWS ONLY or LIMIT n functionality");
            }
            return this;
        }

        /// <summary>
        /// Remove the limit instruction
        /// </summary>
        public SqlStatementBuilder RemoveLimit(out int count)
        {
            this.m_sqlStatement = this.m_sqlStatement.RemoveMatching(Constants.ExtractLimitRegex, out var removed);
            if (removed != null)
            {
                count = Int32.Parse(Constants.ExtractLimitRegex.Match(removed.Sql).Groups[1].Value);
            }
            else
            {
                count = -1;
            }
            return this;

        }

        /// <summary>
        /// Construct an order by
        /// </summary>
        public SqlStatementBuilder OrderBy<TData>(Expression<Func<TData, dynamic>> orderExpression, SortOrderType sortOperation = SortOrderType.OrderBy) => this.OrderBy((LambdaExpression)orderExpression, sortOperation);

        /// <summary>
        /// Using an alias for column references
        /// </summary>
        /// <param name="alias">The alias to use</param>
        public SqlStatementBuilder UsingAlias(String alias)
        {
            this.m_sqlStatement = new SqlStatement(this.m_sqlStatement, alias);
            return this;
        }

        /// <summary>
        /// Construct an order by
        /// </summary>
        public SqlStatementBuilder OrderBy(LambdaExpression orderExpression, SortOrderType sortOperation = SortOrderType.OrderBy)
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
            var prefix =  this.m_sqlStatement.Contains(" ORDER BY ") ? "," : " ORDER BY ";
            this.m_sqlStatement += $"{prefix} {this.m_sqlStatement.Alias ?? orderCol.Table.TableName}.{orderCol.Name} {(sortOperation == SortOrderType.OrderBy ? " ASC " : " DESC ")}";
            return this;
        }

        /// <summary>
        /// Remove the ORDER BY clause
        /// </summary>
        public SqlStatementBuilder RemoveOrderBy(out SqlStatement orderBy)
        {
            this.m_sqlStatement = this.m_sqlStatement.RemoveMatching(Constants.ExtractOrderByRegex, out orderBy);
            return this;
        }

        /// <summary>
        /// Removes the last statement from the list
        /// </summary>
        public SqlStatementBuilder RemoveLast(out SqlStatement lastStatement)
        {
            this.m_sqlStatement = this.m_sqlStatement.RemoveLast(out lastStatement);
            return this;
        }

        /// <summary>
        /// Generate an update statement
        /// </summary>
        public SqlStatementBuilder UpdateSet(Type tableType)
        {
            var tableMap = TableMapping.Get(tableType);
            this.m_sqlStatement += $"UPDATE {tableMap.TableName} SET ";
            return this;
        }
    }

}