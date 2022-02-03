﻿/*
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

using SanteDB.Core.Model;
using SanteDB.Core.Model.Attributes;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Map;
using SanteDB.Core.Model.Query;
using SanteDB.OrmLite.Attributes;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Query predicate part
    /// </summary>
    public enum QueryPredicatePart
    {
        Full = Path | Guard | Cast | SubPath,
        Path = 0x1,
        Guard = 0x2,
        Cast = 0x4,
        SubPath = 0x8,
        PropertyAndGuard = Path | Guard,
        PropertyAndCast = Path | Cast
    }

    /// <summary>
    /// Represents the query predicate
    /// </summary>
    public class QueryPredicate
    {
        // Regex to extract property, guards and cast
        public static readonly Regex ExtractionRegex = new Regex(@"^(\w*?)(\[(.*?)\])?(\@(\w*))?(\.(.*))?$");

        private const int PropertyRegexGroup = 1;
        private const int GuardRegexGroup = 3;
        private const int CastRegexGroup = 5;
        private const int SubPropertyRegexGroup = 7;

        /// <summary>
        /// Gets or sets the path
        /// </summary>
        public String Path { get; private set; }

        /// <summary>
        /// Sub-path
        /// </summary>
        public String SubPath { get; private set; }

        /// <summary>
        /// Cast instruction
        /// </summary>
        public String CastAs { get; private set; }

        /// <summary>
        /// Guard condition
        /// </summary>
        public String Guard { get; private set; }

        /// <summary>
        /// Parse a condition
        /// </summary>
        public static QueryPredicate Parse(String condition)
        {
            var matches = ExtractionRegex.Match(condition);
            if (!matches.Success) return null;

            return new QueryPredicate()
            {
                Path = matches.Groups[PropertyRegexGroup].Value,
                CastAs = matches.Groups[CastRegexGroup].Value,
                Guard = matches.Groups[GuardRegexGroup].Value,
                SubPath = matches.Groups[SubPropertyRegexGroup].Value
            };
        }

        /// <summary>
        /// Represent the predicate as a string
        /// </summary>
        public String ToString(QueryPredicatePart parts)
        {
            StringBuilder sb = new StringBuilder();

            if ((parts & QueryPredicatePart.Path) != 0)
                sb.Append(this.Path);
            if ((parts & QueryPredicatePart.Guard) != 0 && !String.IsNullOrEmpty(this.Guard))
                sb.AppendFormat("[{0}]", this.Guard);
            if ((parts & QueryPredicatePart.Cast) != 0 && !String.IsNullOrEmpty(this.CastAs))
                sb.AppendFormat("@{0}", this.CastAs);
            if ((parts & QueryPredicatePart.SubPath) != 0 && !String.IsNullOrEmpty(this.SubPath))
                sb.AppendFormat("{0}{1}", sb.Length > 0 ? "." : "", this.SubPath);

            return sb.ToString();
        }
    }

    /// <summary>
    /// Query builder for model objects
    /// </summary>
    /// <remarks>
    /// Because the ORM used in the ADO persistence layer is very very lightweight, this query builder exists to parse
    /// LINQ or HTTP query parameters into complex queries which implement joins/CTE/etc. across tables. Stuff that the
    /// classes in the little data model can't possibly support via LINQ expression.
    ///
    /// To use this, simply pass a model based LINQ expression to the CreateQuery method. Examples are in the test project.
    ///
    /// Some reasons to use this:
    ///     - The generated SQL will gather all table instances up the object hierarchy for you (one hit instead of multiple)
    ///     - The queries it writes use efficient CTE tables
    ///     - It can do intelligent join conditions
    ///     - It uses Model LINQ expressions directly to SQL without the need to translate from Model LINQ to Domain LINQ queries
    ///     - It lets us hack the query (via IQueryHack interface) to manually write code
    /// </remarks>
    /// <example lang="cs" name="LINQ Expression illustrating join across tables">
    /// <![CDATA[QueryBuilder.CreateQuery<Patient>(o => o.DeterminerConcept.Mnemonic == "Instance")]]>
    /// </example>
    /// <example lang="sql" name="Resulting SQL query">
    /// <![CDATA[
    /// WITH
    ///     cte0 AS (
    ///         SELECT cd_tbl.cd_id
    ///         FROM cd_vrsn_tbl AS cd_vrsn_tbl
    ///             INNER JOIN cd_tbl AS cd_tbl ON (cd_tbl.cd_id = cd_vrsn_tbl.cd_id)
    ///         WHERE (cd_vrsn_tbl.mnemonic = ? )
    ///     )
    /// SELECT *
    /// FROM pat_tbl AS pat_tbl
    ///     INNER JOIN psn_tbl AS psn_tbl ON (pat_tbl.ent_vrsn_id = psn_tbl.ent_vrsn_id)
    ///     INNER JOIN ent_vrsn_tbl AS ent_vrsn_tbl ON (psn_tbl.ent_vrsn_id = ent_vrsn_tbl.ent_vrsn_id)
    ///     INNER JOIN ent_tbl AS ent_tbl ON (ent_tbl.ent_id = ent_vrsn_tbl.ent_id)
    ///     INNER JOIN cte0 ON (ent_tbl.dtr_cd_id = cte0.cd_id)
    /// ]]>
    /// </example>
    public class QueryBuilder
    {
        // Join cache
        private Dictionary<String, KeyValuePair<SqlStatement, List<TableMapping>>> s_joinCache = new Dictionary<String, KeyValuePair<SqlStatement, List<TableMapping>>>();

        // A list of hacks injected into this query builder
        private static List<IQueryBuilderHack> m_hacks = new List<IQueryBuilderHack>();

        // Mapper
        private ModelMapper m_mapper;

        private IDbProvider m_provider;

        /// <summary>
        /// Provider
        /// </summary>
        public IDbProvider Provider => this.m_provider;

        /// <summary>
        /// Add query builder hacks
        /// </summary>
        public static void AddQueryHacks(IEnumerable<IQueryBuilderHack> hacks)
        {
            m_hacks.AddRange(hacks);
        }

        /// <summary>
        /// Represents model mapper
        /// </summary>
        /// <param name="mapper">The mapper which is used to map types</param>
        /// <param name="provider">The provider which built this query provider</param>
        public QueryBuilder(ModelMapper mapper, IDbProvider provider)
        {
            this.m_mapper = mapper;
            this.m_provider = provider;
        }

        /// <summary>
        /// Create a query
        /// </summary>
        public SqlStatement CreateWhere<TModel>(Expression<Func<TModel, bool>> predicate)
        {
            var nvc = QueryExpressionBuilder.BuildQuery(predicate, true);
            var tableType = m_mapper.MapModelType(typeof(TModel));
            var tableMap = TableMapping.Get(tableType);
            List<TableMapping> scopedTables = new List<TableMapping>() { tableMap };

            return CreateWhereCondition(typeof(TModel), new SqlStatement(m_provider), nvc, String.Empty, scopedTables, null, out IList<SqlStatement> _);
        }

        /// <summary>
        /// Create a query
        /// </summary>
        public SqlStatement CreateQuery<TModel>(Expression<Func<TModel, bool>> predicate, params ColumnMapping[] selector)
        {
            var nvc = QueryExpressionBuilder.BuildQuery(predicate, true);
            return CreateQuery(typeof(TModel), nvc, selector);
        }

        /// <summary>
        /// Create query
        /// </summary>
        public SqlStatement CreateQuery(Type tmodel, IEnumerable<KeyValuePair<String, Object>> query, params ColumnMapping[] selector)
        {
            return CreateQuery(tmodel, query, null, false, null, selector);
        }


        /// <summary>
        /// Query query
        /// </summary>
        /// TODO: Refactor this
        public SqlStatement CreateQuery(Type tmodel, IEnumerable<KeyValuePair<String, Object>> query, String tablePrefix, bool skipJoins, IEnumerable<TableMapping> parentScopedTables, params ColumnMapping[] selector)
        {
            var tableType = m_mapper.MapModelType(tmodel);
            var tableMap = TableMapping.Get(tableType);
            List<TableMapping> scopedTables = new List<TableMapping>() { tableMap };

            bool skipParentJoin = true;
            SqlStatement selectStatement = null;
            Dictionary<Type, TableMapping> skippedJoinMappings = new Dictionary<Type, TableMapping>();

            // Is the query using any of the properties from this table?
            var useKeys = !skipJoins ||
                typeof(IVersionedData).IsAssignableFrom(tmodel) && query.Any(o =>
                {
                    var mPath = this.m_mapper.MapModelProperty(tmodel, tmodel.GetQueryProperty(QueryPredicate.Parse(o.Key).Path));
                    if (mPath == null || mPath.Name == "ObsoletionTime" && o.Value.Equals("null"))
                        return false;
                    else
                        return tableMap.Columns.Any(c => c.SourceProperty == mPath);
                });

            if (skipJoins && !useKeys)
            {
                // If we're skipping joins with a versioned table, then we should really go for the root tablet not the versioned table
                if (typeof(IVersionedData).IsAssignableFrom(tmodel))
                {
                    tableMap = TableMapping.Get(tableMap.Columns.FirstOrDefault(o => o.ForeignKey != null && o.IsAlwaysJoin).ForeignKey.Table);
                    query = query.Where(o => o.Key != "obsoletionTime");
                    scopedTables = new List<TableMapping>() { tableMap };
                }
                selectStatement = new SqlStatement(this.m_provider, $" FROM {tableMap.TableName} AS {tablePrefix}{tableMap.TableName} ");
            }
            else
            {
                //if (!s_joinCache.TryGetValue($"{tablePrefix}.{typeof(TModel).Name}", out cacheHit))
                //{
                selectStatement = new SqlStatement(this.m_provider, $" FROM {tableMap.TableName} AS {tablePrefix}{tableMap.TableName} ");

                Stack<TableMapping> fkStack = new Stack<TableMapping>();
                fkStack.Push(tableMap);

                List<JoinFilterAttribute> joinFilters = new List<JoinFilterAttribute>();

                // Always join tables?
                do
                {
                    var dt = fkStack.Pop();
                    foreach (var jt in dt.Columns.Where(o => o.IsAlwaysJoin))
                    {
                        var fkTbl = TableMapping.Get(jt.ForeignKey.Table);
                        var fkAtt = fkTbl.GetColumn(jt.ForeignKey.Column);

                        // Does the table add nothing?
                        if (fkTbl.Columns.Count() <= 1)
                        {
                            scopedTables.Add(TableMapping.Redirect(fkAtt.Table.OrmType, jt.Table.OrmType));
                            for(int i = 0; i < selector.Length; i++)
                            {
                                if (selector[i].Table?.OrmType == fkTbl.OrmType) {
                                    selector[i] = ColumnMapping.Get(selector[i].Name);
                                }
                            }
                        }
                        else
                        {
                            selectStatement.Append($"INNER JOIN {fkAtt.Table.TableName} AS {tablePrefix}{fkAtt.Table.TableName} ON ({tablePrefix}{jt.Table.TableName}.{jt.Name} = {tablePrefix}{fkAtt.Table.TableName}.{fkAtt.Name} ");

                            foreach (var flt in jt.JoinFilters.Union(joinFilters).GroupBy(o => o.PropertyName).ToArray())
                            {
                                var fltCol = fkTbl.GetColumn(flt.Key);
                                if (fltCol == null)
                                    joinFilters.AddRange(flt);
                                else
                                {
                                    selectStatement.And($"({String.Join(" OR ", flt.Select(o => $"{tablePrefix}{fltCol.Table.TableName}.{fltCol.Name} = '{o.Value}'"))})");
                                    joinFilters.RemoveAll(o => flt.Contains(o));
                                }
                            }

                            selectStatement.Append(")");
                            if (!scopedTables.Contains(fkTbl))
                                fkStack.Push(fkTbl);
                            scopedTables.Add(fkAtt.Table);
                        }
                    }
                } while (fkStack.Count > 0);

                //}
                //else
                //{
                //    selectStatement = cacheHit.Key.Build();
                //    scopedTables = cacheHit.Value;

                // Optimize the join structure - We only join tables where we reference a property in them

                //}
            }

            // Column definitions
            var columnSelector = selector;
            if (selector == null || selector.Length == 0)
            {
                // The SQL Engine being used does not permit duplicate column names that may come from
                // SELECT * in a sub-query, so we should explicitly call out the columns to be safe
                if (this.m_provider.Features.HasFlag(SqlEngineFeatures.StrictSubQueryColumnNames))
                {
                    var existingCols = new List<String>();

                    // Column list of distinct columns
                    var columnList = String.Join(",", scopedTables.SelectMany(o => o.Columns).Where(o =>
                    {
                        if (!existingCols.Contains(o.Name))
                        {
                            existingCols.Add(o.Name);
                            return true;
                        }
                        return false;
                    }).Select(o => $"{tablePrefix}{o.Table.TableName}.{o.Name}"));
                    selectStatement = new SqlStatement(this.m_provider, $"SELECT {columnList} ").Append(selectStatement);
                }
                else
                    selectStatement = new SqlStatement(this.m_provider, $"SELECT *").Append(selectStatement);
                // columnSelector = scopedTables.SelectMany(o => o.Columns).ToArray();
            }
            else if (columnSelector.All(o => o.SourceProperty == null)) // Fake / constants
                selectStatement = new SqlStatement(this.m_provider, $"SELECT {String.Join(",", columnSelector.Select(o => o.Name))} ").Append(selectStatement);
            else
            {
                var columnList = String.Join(",", columnSelector.Select(o =>
                {
                    var rootCol = tableMap.GetColumn(o.SourceProperty);
                    skipParentJoin &= rootCol != null;
                    if (skipParentJoin)
                        return $"{tablePrefix}{rootCol.Table.TableName}.{rootCol.Name}";
                    else
                        return $"{tablePrefix}{o.Table.TableName}.{o.Name}";
                }));
                selectStatement = new SqlStatement(this.m_provider, $"SELECT {columnList} ").Append(selectStatement);
            }

            var whereClause = this.CreateWhereCondition(tmodel, selectStatement, query, tablePrefix, scopedTables, parentScopedTables, out IList<SqlStatement> cteStatements);
            // Return statement
            SqlStatement retVal = new SqlStatement(this.m_provider);
            if (cteStatements.Count > 0)
            {
                retVal.Append("WITH ");
                foreach (var c in cteStatements)
                {
                    retVal.Append(c);
                    if (c != cteStatements.Last())
                        retVal.Append(",");
                }
            }
            retVal.Append(selectStatement.Where(whereClause));

            return retVal;
        }

        /// <summary>
        /// Create a where condition based on the specified parameters
        /// </summary>
        /// <param name="selectStatement">The current select statment</param>
        /// <param name="tmodel">The type of model</param>
        /// <param name="query">The query to create where condition for</param>
        /// <param name="tablePrefix">The prefix of the tables (if applicable)</param>
        /// <param name="scopedTables">The scoped tables</param>
        /// <param name="parentScopedTables">Scoped tables from the parent</param>
        /// <param name="cteStatements">CTE which need to be appended</param>
        private SqlStatement CreateWhereCondition(Type tmodel, SqlStatement selectStatement, IEnumerable<KeyValuePair<String, object>> query, string tablePrefix, IEnumerable<TableMapping> scopedTables, IEnumerable<TableMapping> parentScopedTables, out IList<SqlStatement> cteStatements)
        {
            // We want to process each query and build WHERE clauses - these where clauses are based off of the JSON / XML names
            // on the model, so we have to use those for the time being before translating to SQL
            List<KeyValuePair<String, Object>> workingParameters = new List<KeyValuePair<string, object>>(query);

            // Where clause
            SqlStatement whereClause = new SqlStatement(this.m_provider);
            cteStatements = new List<SqlStatement>();

            // Construct
            while (workingParameters.Count > 0)
            {
                var parm = workingParameters.First();
                workingParameters.RemoveAt(0);

                // Match the regex and process
                var key = parm.Key;
                if (String.IsNullOrEmpty(key))
                    key = "id";

                var propertyPredicate = QueryPredicate.Parse(key);
                if (propertyPredicate == null) throw new ArgumentOutOfRangeException(parm.Key);

                // Next, we want to construct the other parms
                var otherParms = workingParameters.Where(o => QueryPredicate.Parse(o.Key).ToString(QueryPredicatePart.PropertyAndCast) == propertyPredicate.ToString(QueryPredicatePart.PropertyAndCast)).ToArray();

                // Remove the working parameters if the column is FK then all parameters
                if (otherParms.Any() || !String.IsNullOrEmpty(propertyPredicate.Guard) || !String.IsNullOrEmpty(propertyPredicate.SubPath))
                {
                    foreach (var o in otherParms)
                        workingParameters.Remove(o);

                    // We need to do a sub query

                    IEnumerable<KeyValuePair<String, Object>> queryParms = new List<KeyValuePair<String, Object>>() { parm }.Union(otherParms);

                    // Grab the appropriate builder
                    var subProp = tmodel.GetQueryProperty(propertyPredicate.Path, true);
                    if (subProp == null) throw new MissingMemberException(propertyPredicate.Path);

                    // Link to this table in the other?
                    // Allow hacking of the query before we get to the auto-generated stuff
                    if (!m_hacks.Any(o => o.HackQuery(this, selectStatement, whereClause, tmodel, subProp, tablePrefix, propertyPredicate, parm.Value, scopedTables, queryParms.ToArray())))
                    {
                        // Is this a collection?
                        if (typeof(IList).IsAssignableFrom(subProp.PropertyType)) // Other table points at this on
                        {
                            var propertyType = subProp.PropertyType.StripGeneric();
                            // map and get ORM def'n
                            var subTableType = m_mapper.MapModelType(propertyType);
                            var subTableMap = TableMapping.Get(subTableType);
                            var linkColumns = subTableMap.Columns.Where(o => scopedTables.Any(s => s.OrmType == o.ForeignKey?.Table));

                            //var linkColumn = linkColumns.Count() > 1 ? linkColumns.FirstOrDefault(o=>o.SourceProperty.Name == "SourceKey") : linkColumns.FirstOrDefault();
                            var linkColumn = linkColumns.Count() > 1 ? linkColumns.FirstOrDefault(o => propertyPredicate.SubPath.StartsWith("source") ? o.SourceProperty.Name != "SourceKey" : o.SourceProperty.Name == "SourceKey") : linkColumns.FirstOrDefault();

                            // Link column is null, is there an assoc attrib?
                            SqlStatement subQueryStatement = new SqlStatement(this.m_provider);

                            var subTableColumn = linkColumn;
                            string existsClause = String.Empty;
                            var lnkPfx = IncrementSubQueryAlias(tablePrefix);

                            if (linkColumn == null || scopedTables.Any(o => o.AssociationWith(subTableMap) != null)) // Or there is a better linker
                            {
                                var tableWithJoin = scopedTables.Select(o => o.AssociationWith(subTableMap)).FirstOrDefault(o => o != null);
                                linkColumn = tableWithJoin.Columns.SingleOrDefault(o => scopedTables.Any(s => s.OrmType == o.ForeignKey?.Table));
                                var targetColumn = tableWithJoin.Columns.SingleOrDefault(o => o.ForeignKey?.Table == subTableMap.OrmType);
                                subTableColumn = subTableMap.GetColumn(targetColumn.ForeignKey.Column);
                                // The sub-query statement needs to be joined as well
                                subQueryStatement.Append($"SELECT 1 FROM {tableWithJoin.TableName} AS {lnkPfx}{tableWithJoin.TableName} WHERE ");
                                existsClause = $"{lnkPfx}{tableWithJoin.TableName}.{targetColumn.Name}";
                                //throw new InvalidOperationException($"Cannot find foreign key reference to table {tableMap.TableName} in {subTableMap.TableName}");
                            }

                            var localTable = scopedTables.Where(o => o.GetColumn(linkColumn.ForeignKey.Column) != null).FirstOrDefault();

                            if (String.IsNullOrEmpty(existsClause))
                                existsClause = $"{tablePrefix}{localTable.TableName}.{localTable.GetColumn(linkColumn.ForeignKey.Column).Name}";

                            var guardConditions = queryParms.GroupBy(o => QueryPredicate.Parse(o.Key).Guard);

                            foreach (var guardClause in guardConditions)
                            {
                                var subQuery = guardClause.Select(o => new KeyValuePair<String, Object>(QueryPredicate.Parse(o.Key).ToString(QueryPredicatePart.SubPath), o.Value)).ToList();

                                // TODO: GUARD CONDITION HERE!!!!
                                if (!String.IsNullOrEmpty(guardClause.Key))
                                {
                                    StringBuilder guardCondition = new StringBuilder();
                                    var clsModel = propertyType;
                                    while (clsModel.GetCustomAttribute<ClassifierAttribute>() != null)
                                    {
                                        var clsProperty = clsModel.GetRuntimeProperty(clsModel.GetCustomAttribute<ClassifierAttribute>().ClassifierProperty);
                                        clsModel = clsProperty.PropertyType.StripGeneric();
                                        var redirectProperty = clsProperty.GetCustomAttribute<SerializationReferenceAttribute>()?.RedirectProperty;
                                        if (redirectProperty != null)
                                            clsProperty = clsProperty.DeclaringType.GetRuntimeProperty(redirectProperty);

                                        // Is this a uuid?
                                        guardCondition.Append(clsProperty.GetSerializationName());
                                        if (guardClause.Key.Split('|').All(o => Guid.TryParse(o, out Guid _)))
                                            break;
                                        else
                                        {
                                            if (typeof(IdentifiedData).IsAssignableFrom(clsModel))
                                                guardCondition.Append(".");
                                        }
                                    }
                                    subQuery.Add(new KeyValuePair<string, object>(guardCondition.ToString(), guardClause.Key.Split('|')));

                                    // Filter by effective version
                                    if (typeof(IVersionedAssociation).IsAssignableFrom(clsModel))
                                        subQuery.Add(new KeyValuePair<string, object>("obsoleteVersionSequence", new String[] { "null" }));
                                }

                                // Generate method
                                subQuery.RemoveAll(o => String.IsNullOrEmpty(o.Key));
                                var prefix = IncrementSubQueryAlias(tablePrefix);

                                // Sub path is specified
                                if (String.IsNullOrEmpty(propertyPredicate.SubPath) && "null".Equals(parm.Value))
                                    subQueryStatement.And($"NOT EXISTS (");
                                // Query Optimization - Sub-Path is specfified and the only object is a NOT value (other than classifier)
                                else if (!String.IsNullOrEmpty(propertyPredicate.SubPath) &&
                                    subQuery.Count <= 2 &&
                                    subQuery.Count(p =>
                                        !p.Key.Contains(".") && (
                                        (p.Value as String)?.StartsWith("!") == true ||
                                        (p.Value as List<String>)?.All(v => v.StartsWith("!")) == true)) == 1)
                                {
                                    subQueryStatement.And($"NOT EXISTS (");
                                    subQuery = subQuery.Select(a =>
                                    {
                                        if ((a.Value as String)?.StartsWith("!") == true)
                                            return new KeyValuePair<String, Object>(a.Key, (a.Value as String)?.Substring(1));
                                        else if ((a.Value as List<String>)?.All(v => v.StartsWith("!")) == true)
                                            return new KeyValuePair<string, object>(a.Key, (a.Value as List<String>)?.Select(o => o.Substring(1)).ToList());
                                        else
                                            return a;
                                    }).ToList();
                                }
                                else
                                    subQueryStatement.And($"EXISTS (");

                                // Does this query object have obsolete version sequence?
                                if (typeof(IVersionedAssociation).IsAssignableFrom(propertyType)) // Add obslt guard
                                {
                                    subQuery.Add(new KeyValuePair<string, object>(propertyType.GetRuntimeProperty(nameof(IVersionedAssociation.ObsoleteVersionSequenceId)).GetSerializationName(), "null"));
                                }

                                var genMethod = typeof(QueryBuilder).GetGenericMethod("CreateQuery", new Type[] { propertyType }, new Type[] { subQuery.GetType(), typeof(String), typeof(bool), typeof(List<TableMapping>), typeof(ModelSort<>).MakeGenericType(propertyType).MakeArrayType(), typeof(ColumnMapping[]) });

                                if (subQuery.Count(p => !p.Key.Contains(".")) == 0)
                                    subQueryStatement.Append(this.CreateQuery(propertyType, subQuery.Distinct(), prefix, true, scopedTables, new ColumnMapping[] { ColumnMapping.One }));
                                else
                                    subQueryStatement.Append(this.CreateQuery(propertyType, subQuery.Distinct(), prefix, false, scopedTables, new ColumnMapping[] { ColumnMapping.One }));

                                subQueryStatement.And($"{existsClause} = {prefix}{subTableMap.TableName}.{subTableColumn.Name}");
                                //existsClause = $"{prefix}{subTableColumn.Table.TableName}.{subTableColumn.Name}";

                                subQueryStatement.Append(")");
                            }

                            if (subTableColumn != linkColumn)
                                whereClause.And($"EXISTS (").Append(subQueryStatement).And($"{tablePrefix}{localTable.TableName}.{localTable.GetColumn(linkColumn.ForeignKey.Column).Name} = {lnkPfx}{linkColumn.Table.TableName}.{linkColumn.Name}").Append(")");
                            else
                                whereClause.And(subQueryStatement);
                        }
                        else  // this table points at other
                        {
                            var subQuery = queryParms.Select(o => new KeyValuePair<String, Object>(QueryPredicate.Parse(o.Key).ToString(QueryPredicatePart.SubPath), o.Value)).ToList();

                            if (!subQuery.Any(o => o.Key == "obsoletionTime") && typeof(IBaseData).IsAssignableFrom(subProp.PropertyType))
                                subQuery.Add(new KeyValuePair<string, object>("obsoletionTime", "null"));

                            TableMapping tableMapping = null;
                            var subPropKey = tmodel.GetQueryProperty(propertyPredicate.Path);

                            // Get column info
                            PropertyInfo domainProperty = scopedTables.Select(o => { tableMapping = o; return m_mapper.MapModelProperty(tmodel, o.OrmType, subPropKey); })?.FirstOrDefault(o => o != null);
                            ColumnMapping linkColumn = null;
                            // If the domain property is not set, we may have to infer the link
                            if (domainProperty == null)
                            {
                                var subPropType = m_mapper.MapModelType(subProp.PropertyType);
                                // We find the first column with a foreign key that points to the other !!!
                                linkColumn = scopedTables.SelectMany(o => o.Columns).FirstOrDefault(o => o.ForeignKey?.Table == subPropType);
                            }
                            else
                                linkColumn = tableMapping.GetColumn(domainProperty);

                            var fkTableDef = parentScopedTables?.FirstOrDefault(o => o.OrmType == linkColumn.ForeignKey.Table) ?? TableMapping.Get(linkColumn.ForeignKey.Table);
                            var fkColumnDef = fkTableDef.GetColumn(linkColumn.ForeignKey.Column);
                            var prefix = IncrementSubQueryAlias(tablePrefix);

                            // Create the sub-query
                            //var genMethod = typeof(QueryBuilder).GetGenericMethod("CreateQuery", new Type[] { subProp.PropertyType }, new Type[] { subQuery.GetType(), typeof(ColumnMapping[]) });
                            //SqlStatement subQueryStatement = genMethod.Invoke(this, new Object[] { subQuery, new ColumnMapping[] { fkColumnDef } }) as SqlStatement;
                            SqlStatement subQueryStatement = null;
                            var subSkipJoins = subQuery.Count(o => !o.Key.Contains(".") && o.Key != "obsoletionTime") == 0;
                            if (String.IsNullOrEmpty(propertyPredicate.CastAs))
                            {
                                subQueryStatement = this.CreateQuery(subProp.PropertyType, subQuery, prefix, subSkipJoins, scopedTables, new ColumnMapping[] { fkColumnDef });
                            }
                            else // we need to cast!
                            {
                                var castAsType = new SanteDB.Core.Model.Serialization.ModelSerializationBinder().BindToType("SanteDB.Core.Model", propertyPredicate.CastAs);
                                subQueryStatement = this.CreateQuery(castAsType, subQuery, prefix, false, scopedTables, new ColumnMapping[] { fkColumnDef });
                            }

                            //cteStatements.Add(new SqlStatement(this.m_provider, $"{tablePrefix}cte{cteStatements.Count} AS (").Append(subQueryStatement).Append(")"));
                            //subQueryStatement.And($"{tablePrefix}{tableMapping.TableName}.{linkColumn.Name} = {sqName}{fkTableDef.TableName}.{fkColumnDef.Name} ");

                            // Join up to the parent table

                            whereClause.And($" {tablePrefix}{tableMapping.TableName}.{linkColumn.Name} IN (").Append(subQueryStatement).Append(")");
                        }
                    }
                }
                else if (!m_hacks.Any(o => o.HackQuery(this, selectStatement, whereClause, tmodel, tmodel.GetQueryProperty(propertyPredicate.Path), tablePrefix, propertyPredicate, parm.Value, scopedTables, parm)))
                    whereClause.And(CreateWhereCondition(tmodel, propertyPredicate.Path, parm.Value, tablePrefix, scopedTables));
            }

            return whereClause;
        }

        /// <summary>
        /// Create the order by clause
        /// </summary>
        /// <param name="tmodel">The type of model</param>
        /// <param name="tablePrefix">The prefix that the table has</param>
        /// <param name="scopedTables">The tables which are scoped</param>
        /// <param name="sortExpression">The sorting expression</param>
        private SqlStatement CreateOrderBy(Type tmodel, string tablePrefix, IEnumerable<TableMapping> scopedTables, Expression sortExpression, SortOrderType order)
        {
            switch (sortExpression.NodeType)
            {
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    return this.CreateOrderBy(tmodel, tablePrefix, scopedTables, ((UnaryExpression)sortExpression).Operand, order);

                case ExpressionType.MemberAccess:
                    var mexpr = (MemberExpression)sortExpression;

                    // Determine the parameter type
                    if (mexpr.Expression.NodeType != ExpressionType.Parameter)
                        throw new InvalidOperationException("OrderBy can only be performed on primary properties of the object");

                    // Determine the map
                    var tableMapping = scopedTables.First();
                    var propertyInfo = mexpr.Member as PropertyInfo;
                    PropertyInfo domainProperty = scopedTables.Select(o => { tableMapping = o; return m_mapper.MapModelProperty(tmodel, o.OrmType, propertyInfo); }).FirstOrDefault(o => o != null);
                    var columnData = tableMapping.GetColumn(domainProperty);
                    return new SqlStatement(this.m_provider, $" {columnData.Name} {(order == SortOrderType.OrderBy ? "ASC" : "DESC")}");

                default:
                    throw new InvalidOperationException("Cannot sort by this property expression");
            }
        }

        /// <summary>
        /// Increment sub-query alias
        /// </summary>
        private static String IncrementSubQueryAlias(string tablePrefix)
        {
            if (String.IsNullOrEmpty(tablePrefix))
                return "sq0";
            else
            {
                int sq = 0;
                if (Int32.TryParse(tablePrefix.Substring(2), out sq))
                    return "sq" + (sq + 1);
                else
                    return "sq0";
            }
        }

        /// <summary>
        /// Create a where condition
        /// </summary>
        public SqlStatement CreateWhereCondition(Type tmodel, String propertyPath, Object value, String tablePrefix, IEnumerable<TableMapping> scopedTables)
        {
            // Map the type
            var tableMapping = scopedTables.First();
            var propertyInfo = tmodel.GetQueryProperty(propertyPath);
            if (propertyInfo == null)
                throw new ArgumentOutOfRangeException(propertyPath);

            PropertyInfo domainProperty = scopedTables.Select(o => { tableMapping = o; return m_mapper.MapModelProperty(tmodel, o.OrmType, propertyInfo); }).FirstOrDefault(o => o != null);

            // Now map the property path
            var tableAlias = $"{tablePrefix}{tableMapping.TableName}";
            Guid pkey = Guid.Empty;

            var sValue = value.ToString();
            if (value is IList)
            {
                var vals = (value as IEnumerable).OfType<Object>().Where(s => !"!null".Equals(s));
                if (vals.Any())
                    sValue = vals.First().ToString();
            }

            if (domainProperty == null && Guid.TryParse(sValue, out pkey))
            {
                domainProperty = tableMapping.PrimaryKey.First().SourceProperty;
                // Link property to the key
                propertyInfo = tmodel.GetProperty(propertyInfo.Name + "Key");
            }
            else if (domainProperty == null)
                throw new ArgumentException($"Can't find SQL based property for {propertyPath} on {tableMapping.TableName}");
            var columnData = tableMapping.GetColumn(domainProperty);

            // List of parameters
            var lValue = value as IList;
            if (lValue == null)
                lValue = new List<Object>() { value };

            return CreateSqlPredicate(tableAlias, columnData.Name, propertyInfo, lValue);
        }

        /// <summary>
        /// Create the actual SQL predicate
        /// </summary>
        /// <param name="tableAlias">The alias for the table on which the predicate is based</param>
        /// <param name="modelProperty">The model property information for type information</param>
        /// <param name="columnName">The column data for the data model</param>
        /// <param name="values">The values to be matched</param>
        public SqlStatement CreateSqlPredicate(String tableAlias, String columnName, PropertyInfo modelProperty, IList values)
        {
            var retVal = new SqlStatement(this.m_provider);

            bool noCase = modelProperty.GetCustomAttribute<NoCaseAttribute>() != null;
            string parmValue = noCase ? $"{this.m_provider.CreateSqlKeyword(SqlKeyword.Lower)}(?)" : "?";
            retVal.Append("(");
            for (var i = 0; i < values.Count; i++)
            {
                var itm = values[i];
                if (noCase)
                    retVal.Append($"{this.m_provider.CreateSqlKeyword(SqlKeyword.Lower)}({tableAlias}.{columnName})");
                else
                    retVal.Append($"{tableAlias}.{columnName}");
                var semantic = " OR ";
                var iValue = itm;
                if (iValue is String)
                {
                    var sValue = itm as String;
                    switch (sValue[0])
                    {
                        case ':': // function
                            var opMatch = QueryFilterExtensions.ExtendedFilterRegex.Match(sValue);
                            if (opMatch.Success)
                            {
                                // Extract
                                String fnName = opMatch.Groups[1].Value,
                                    parms = opMatch.Groups[3].Value,
                                    operand = opMatch.Groups[4].Value;

                                List<String> extendedParms = new List<string>();
                                var parmExtract = QueryFilterExtensions.ParameterExtractRegex.Match(parms + ",");
                                while (parmExtract.Success)
                                {
                                    extendedParms.Add(parmExtract.Groups[1].Value);
                                    parmExtract = QueryFilterExtensions.ParameterExtractRegex.Match(parmExtract.Groups[2].Value);
                                }
                                // Now find the function
                                var filterFn = this.m_provider.GetFilterFunction(fnName);
                                if (filterFn == null)
                                    retVal.Append($" = {parmValue} ", CreateParameterValue(sValue, modelProperty.PropertyType));
                                else
                                {
                                    retVal.RemoveLast();
                                    retVal = filterFn.CreateSqlStatement(retVal, $"{tableAlias}.{columnName}", extendedParms.ToArray(), operand, modelProperty.PropertyType).Build();
                                }
                            }
                            else
                                retVal.Append($" = {parmValue} ", CreateParameterValue(sValue, modelProperty.PropertyType));
                            break;

                        case '<':
                            semantic = " AND ";
                            if (sValue[1] == '=')
                                retVal.Append($" <= {parmValue}", CreateParameterValue(sValue.Substring(2), modelProperty.PropertyType));
                            else
                                retVal.Append($" < {parmValue}", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;

                        case '>':
                            // peek the next value and see if it is < then we use BETWEEN
                            if (i < values.Count - 1 && values[i + 1].ToString().StartsWith("<"))
                            {
                                object lower = null, upper = null;
                                if (sValue[1] == '=')
                                    lower = CreateParameterValue(sValue.Substring(2), modelProperty.PropertyType);
                                else
                                    lower = CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType);
                                sValue = values[++i].ToString();
                                if (sValue[1] == '=')
                                    upper = CreateParameterValue(sValue.Substring(2), modelProperty.PropertyType);
                                else
                                    upper = CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType);
                                semantic = " OR ";
                                retVal.Append($" BETWEEN {parmValue} AND {parmValue}", lower, upper);
                            }
                            else
                            {
                                semantic = " AND ";
                                if (sValue[1] == '=')
                                    retVal.Append($" >= {parmValue}", CreateParameterValue(sValue.Substring(2), modelProperty.PropertyType));
                                else
                                    retVal.Append($" > {parmValue}", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            }
                            break;

                        case '!':
                            semantic = " AND ";
                            if (sValue.Equals("!null"))
                                retVal.Append(" IS NOT NULL");
                            else
                                retVal.Append($" <> {parmValue}", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;

                        case '~':
                            retVal.Append($" {this.m_provider.CreateSqlKeyword(SqlKeyword.ILike)} '%' || {parmValue} || '%'", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;

                        case '^':
                            retVal.Append($" {this.m_provider.CreateSqlKeyword(SqlKeyword.ILike)} {parmValue} || '%'", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;

                        case '$':
                            retVal.Append($" {this.m_provider.CreateSqlKeyword(SqlKeyword.ILike)} '%' || {parmValue}", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;

                        default:
                            if (sValue.Equals("null"))
                                retVal.Append(" IS NULL");
                            else
                                retVal.Append($" = {parmValue} ", CreateParameterValue(sValue, modelProperty.PropertyType));
                            break;
                    }
                }
                else
                    retVal.Append($" = {parmValue} ", CreateParameterValue(iValue, modelProperty.PropertyType));

                if (i < values.Count - 1)
                    retVal.Append(semantic);
            }

            retVal.Append(")");

            return retVal;
        }

        /// <summary>
        /// Create a parameter value
        /// </summary>
        public static object CreateParameterValue(object value, Type toType)
        {
            if (value is String str)
            {
                if (str.Length > 1 && str.StartsWith("\"") && str.EndsWith(("\"")))
                {
                    value= str.Substring(1, str.Length - 2).Replace("\\\"", "\"");
                }
                else if (str.Equals("null", StringComparison.OrdinalIgnoreCase))
                {
                    return DBNull.Value;
                }
                
            }
            
            if (value.GetType() == toType ||
                value.GetType() == toType.StripNullable())
                return value;
            else if (MapUtil.TryConvert(value, toType, out var retVal))
                return retVal;
            else
                throw new ArgumentOutOfRangeException(value.ToString());
        }
    }
}