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
using SanteDB.OrmLite.Providers;
using System;
using System.Collections;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SanteDB.OrmLite
{

    /// <summary>
    /// String etensions
    /// </summary>
    public static class StringExtensions
    {
        /// <summary>
        ///  True if case should be ignored
        /// </summary>
        public static string IgnoreCase(this String me)
        {
            return me;
        }

    }
    /// <summary>
    /// Postgresql query expression builder
    /// </summary>
    public class SqlQueryExpressionBuilder : ExpressionVisitor
    {
        private string m_tableAlias = null;
        private SqlStatement m_sqlStatement = null;
        private IDbProvider m_provider;

        /// <summary>
        /// Gets the constructed SQL statement
        /// </summary>
        public SqlStatement SqlStatement { get { return this.m_sqlStatement.Build(); } }

        /// <summary>
        /// Creates a new postgresql query expression builder
        /// </summary>
        public SqlQueryExpressionBuilder(String alias, IDbProvider provider)
        {
            this.m_tableAlias = alias;
            this.m_sqlStatement = new SqlStatement(this.m_provider);
            this.m_provider = provider;
        }

        /// <summary>
        /// Visit a query expression
        /// </summary>
        /// <returns>The modified expression list, if any one of the elements were modified; otherwise, returns the original
        /// expression list.</returns>
        /// <param name="nodes">The expressions to visit.</param>
        /// <param name="node">Node.</param>
        public override Expression Visit(Expression node)
        {
            if (node == null)
                return node;
            else if (node.CanReduce)
                node = node.Reduce();

            // Convert node type
            switch (node.NodeType)
            {
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.NotEqual:
                case ExpressionType.Equal:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.Coalesce:
                    return this.VisitBinary((BinaryExpression)node);
                case ExpressionType.MemberAccess:
                    return this.VisitMemberAccess((MemberExpression)node);
                case ExpressionType.Parameter:
                    return this.VisitParameter((ParameterExpression)node);
                case ExpressionType.Call:
                    return this.VisitMethodCall((MethodCallExpression)node);
                case ExpressionType.Constant:
                    return this.VisitConstant((ConstantExpression)node);
                case ExpressionType.Convert:
                case ExpressionType.Not:
                case ExpressionType.Negate:
                case ExpressionType.TypeAs:
                    return this.VisitUnary((UnaryExpression)node);

                default:
                    return base.Visit(node);
            }
        }

        /// <summary>
        /// Visit constant
        /// </summary>
        protected override Expression VisitConstant(ConstantExpression node)
        {
            this.m_sqlStatement.Append(" ? ", node.Value);
            return node;
        }

        /// <summary>
        /// Visits a unary member expression
        /// </summary>
        protected override Expression VisitUnary(UnaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.Negate:
                    this.m_sqlStatement.Append(" -");
                    break;
                case ExpressionType.Not:
                    this.m_sqlStatement.Append(" NOT (");
                    this.Visit(node.Operand);
                    this.m_sqlStatement.Append(")");
                    return node;
                case ExpressionType.Convert:
                    break;
                case ExpressionType.TypeAs:
                    break;
                default:
                    return null;
            }

            this.Visit(node.Operand);
            return node;
        }

        /// <summary>
        /// Visit binary expression
        /// </summary>
        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.NodeType == ExpressionType.Coalesce)
                this.m_sqlStatement.Append(" COALESCE");

            this.m_sqlStatement.Append("(");
            this.Visit(node.Left);

            bool skipRight = false;

            switch (node.NodeType)
            {
                case ExpressionType.Equal:
                    {
                        var cexpr = this.ExtractConstantExpression(node.Right);
                        if (cexpr != null && cexpr.Value == null)
                        {
                            skipRight = true;
                            this.m_sqlStatement.Append(" IS NULL ");
                        }
                        else
                            this.m_sqlStatement.Append(" = ");
                        break;
                    }
                case ExpressionType.NotEqual:
                    {
                        var cexpr = this.ExtractConstantExpression(node.Right);
                        if (cexpr != null && cexpr.Value == null)
                        {
                            skipRight = true;
                            this.m_sqlStatement.Append(" IS NOT NULL ");
                        }
                        else
                            this.m_sqlStatement.Append(" <> ");
                        break;
                    }
                case ExpressionType.GreaterThan:
                    this.m_sqlStatement.Append(" > ");
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    this.m_sqlStatement.Append(" >= ");
                    break;
                case ExpressionType.LessThan:
                    this.m_sqlStatement.Append(" < ");
                    break;
                case ExpressionType.LessThanOrEqual:
                    this.m_sqlStatement.Append(" <= ");
                    break;
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    this.m_sqlStatement.Append(" AND ");
                    break;
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    this.m_sqlStatement.Append(" OR ");
                    break;
                case ExpressionType.Coalesce:
                    this.m_sqlStatement.Append(",");
                    break;
            }

            if (!skipRight)
                this.Visit(node.Right);

            this.m_sqlStatement.Append(")");
            return node;
        }


        /// <summary>
        /// Visit a parameter reference
        /// </summary>
        protected override Expression VisitParameter(ParameterExpression node)
        {
            var tableMap = TableMapping.Get(node.Type);
            this.m_sqlStatement.Append($"{this.m_tableAlias ?? tableMap.TableName}");
            return node;
        }

        /// <summary>
        /// Visit method call
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {

            // Method names
            switch (node.Method.Name)
            {
                case "StartsWith":
                    this.Visit(node.Object);
                    this.m_sqlStatement.Append(this.m_provider.CreateSqlKeyword(SqlKeyword.ILike));
                    this.Visit(node.Arguments[0]);
                    this.m_sqlStatement.Append(" || '%' ");
                    break;
                case "EndsWith":
                    this.Visit(node.Object);
                    this.m_sqlStatement.Append(this.m_provider.CreateSqlKeyword(SqlKeyword.ILike));
                    this.m_sqlStatement.Append(" '%' || ");
                    this.Visit(node.Arguments[0]);
                    break;
                case "Contains":

                    // Determine the defining type
                    if (node.Method.DeclaringType == typeof(Enumerable)) // is a LINQ CONTAINS() 
                    {
                        Expression enumerable = node.Arguments[0],
                            contained = node.Arguments[1];

                        var value = this.GetConstantValue(enumerable) as IEnumerable;
                        if (value.OfType<Object>().Count() == 0)
                            this.m_sqlStatement.Append(this.m_provider.CreateSqlKeyword(SqlKeyword.False));
                        else
                        {
                            this.Visit(contained);
                            this.m_sqlStatement.Append(" IN (");

                            this.m_sqlStatement.Append(String.Join(",", value.OfType<Object>().Select(o => "?")), value.OfType<Object>().ToArray());

                            this.m_sqlStatement.Append(")");
                        }
                    }
                    else if (node.Method.DeclaringType == typeof(String)) // is a STRING contains()
                    {
                        this.Visit(node.Object);
                        this.m_sqlStatement.Append(this.m_provider.CreateSqlKeyword(SqlKeyword.ILike));

                        this.m_sqlStatement.Append(" '%' || ");

                        if (node.Object?.NodeType == ExpressionType.Call &&
                            (node.Object as MethodCallExpression).Method.Name == "ToLower") // We must apply the same call
                        {
                            this.m_sqlStatement.Append(this.m_provider.CreateSqlKeyword(SqlKeyword.Lower)).Append("(");
                            this.Visit(node.Arguments[0]);
                            this.m_sqlStatement.Append(")");
                        }
                        else
                            this.Visit(node.Arguments[0]);

                        this.m_sqlStatement.Append(" || '%' ");
                    }
                    break;
                case "ToLower":
                case "ToLowerInvariant":
                    this.m_sqlStatement.Append(this.m_provider.CreateSqlKeyword(SqlKeyword.Lower));
                    this.m_sqlStatement.Append("(");
                    this.Visit(node.Object);
                    this.m_sqlStatement.Append(") ");
                    break;
                case "ToUpper":
                case "ToUpperInvariant":
                    this.m_sqlStatement.Append(this.m_provider.CreateSqlKeyword(SqlKeyword.Upper));
                    this.m_sqlStatement.Append("(");
                    this.Visit(node.Object);
                    this.m_sqlStatement.Append(") ");
                    break;
                case "NewGuid":
                    this.m_sqlStatement.Append("uuid_generate_v4() ");
                    break;
                case "IgnoreCase":
                    this.Visit(node.Arguments[0]);
                    this.m_sqlStatement.Append("::citext ");
                    break;
                case "HasValue":
                    this.Visit(node.Object);
                    this.m_sqlStatement.Append(" IS NOT NULL ");
                    break;
                default:
                    throw new NotSupportedException(node.Method.Name);
            }
            return node;
        }

        /// <summary>
        /// Attempt to get constant value
        /// </summary>
        private Object GetConstantValue(Expression expression)
        {
            if (expression == null)
                return null;
            else if (expression is ConstantExpression)
                return (expression as ConstantExpression).Value;
            else if (expression is UnaryExpression)
            {
                var un = expression as UnaryExpression;
                switch (expression.NodeType)
                {
                    case ExpressionType.TypeAs:
                        return this.GetConstantValue(un.Operand);
                    case ExpressionType.Convert:
                        return this.GetConstantValue(un.Operand);
                    default:
                        throw new InvalidOperationException($"Expression {expression} not supported for constant extraction");
                }
            }
            else if (expression is MemberExpression)
            {
                var mem = expression as MemberExpression;
                var obj = this.GetConstantValue(mem.Expression);
                if (mem.Member is PropertyInfo)
                    return (mem.Member as PropertyInfo).GetValue(obj);
                else if (mem.Member is FieldInfo)
                    return (mem.Member as FieldInfo).GetValue(obj);
                else
                    throw new NotSupportedException();
            }
            else
                throw new InvalidOperationException($"Expression {expression} not supported for constant extraction");

        }

        /// <summary>
        /// Extract constant expression
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public ConstantExpression ExtractConstantExpression(Expression e)
        {
            if (e.NodeType == ExpressionType.TypeAs || e.NodeType == ExpressionType.Convert)
                return this.ExtractConstantExpression((e as UnaryExpression).Operand);
            else if(e.NodeType == ExpressionType.MemberAccess && e is MemberExpression memExpr)
            {
                if(memExpr.Expression == null) // Constant
                {
                    if (memExpr.Member is FieldInfo fi)
                        return Expression.Constant(fi.GetValue(null));
                    else if (memExpr.Member is PropertyInfo pi)
                        return Expression.Constant(pi.GetValue(null));
                    else if (memExpr.Member is MethodInfo mi)
                        return Expression.Constant(mi.Invoke(null, new object[0]));
                }
                var baseExpr = this.ExtractConstantExpression(memExpr.Expression);
                if (memExpr.Member is PropertyInfo propInfo)
                    return Expression.Constant(propInfo.GetValue(baseExpr.Value));
                else if(memExpr.Member is FieldInfo fieldInfo)
                    return Expression.Constant(fieldInfo.GetValue(baseExpr.Value));

            }
            return e as ConstantExpression;
        }

        /// <summary>
        /// Visit member access
        /// </summary>
        private Expression VisitMemberAccess(MemberExpression node)
        {
            switch (node.Member.Name)
            {
                case "Now":
                    this.m_sqlStatement.Append(" CURRENT_TIMESTAMP ");
                    break;
                case "NewGuid":
                    this.m_sqlStatement.Append(" ? ", Guid.NewGuid());
                    break;
                case "HasValue":
                    this.Visit(node.Expression);
                    this.m_sqlStatement.Append(" IS NOT NULL ");
                    break;
                
                default:
                   
                    if (node.Expression != null)
                    {
                        if (node.Expression.Type.IsGenericType &&
                       node.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                            this.Visit(node.Expression);
                        else
                        {
                            var expr = node.Expression;
                            while (expr.NodeType == ExpressionType.Convert)
                                expr = (expr as UnaryExpression)?.Operand;
                            // Ignore typeas
                            switch (expr.NodeType)
                            {
                                case ExpressionType.Parameter:
                                    // Translate
                                    var tableMap = TableMapping.Get(expr.Type);
                                    var columnMap = tableMap.GetColumn(node.Member);
                                    this.Visit(expr);
                                    this.m_sqlStatement.Append($".{columnMap.Name}");
                                    break;
                                case ExpressionType.Constant:
                                case ExpressionType.TypeAs:
                                case ExpressionType.MemberAccess:
                                    // Ok, this is a constant member access.. so ets get the value
                                    var cons = this.GetConstantValue(expr);
                                    if (node.Member is PropertyInfo)
                                    {
                                        var value = (node.Member as PropertyInfo).GetValue(cons);
                                        if (value == null)
                                        {
                                            var stmt = this.m_sqlStatement.RemoveLast().SQL.Trim();
                                            if (stmt == "<>")
                                                this.m_sqlStatement.Append(" IS NOT NULL ");
                                            else if (stmt == "=")
                                                this.m_sqlStatement.Append(" IS NULL ");
                                            else
                                                throw new InvalidOperationException($"Cannot determine how to convert {node} in SQL");
                                        }
                                        else
                                            this.m_sqlStatement.Append(" ? ", value);
                                    }
                                    else if (node.Member is FieldInfo)
                                    {
                                        var value = (node.Member as FieldInfo).GetValue(cons);
                                        if (value == null)
                                        {
                                            var stmt = this.m_sqlStatement.RemoveLast().SQL.Trim();
                                            if (stmt == "<>")
                                                this.m_sqlStatement.Append(" IS NOT NULL ");
                                            else
                                                this.m_sqlStatement.Append(" IS NULL ");
                                        }
                                        else
                                            this.m_sqlStatement.Append(" ? ", value);
                                    }
                                    else
                                        throw new NotSupportedException();
                                    break;
                            }
                        }
                    }
                    else // constant expression
                    {
                        if (node.Member is PropertyInfo)
                            this.m_sqlStatement.Append(" ? ", (node.Member as PropertyInfo).GetValue(null));
                        else if (node.Member is FieldInfo)
                            this.m_sqlStatement.Append(" ? ", (node.Member as FieldInfo).GetValue(null));
                    }
                    break;
            }

            // Member expression is node... This has the limitation of only going one deep :/

            return node;
        }
    }

}
