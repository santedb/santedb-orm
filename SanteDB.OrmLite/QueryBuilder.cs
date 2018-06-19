﻿/*
 * Copyright 2015-2018 Mohawk College of Applied Arts and Technology
 *
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
 * Date: 2017-9-1
 */
using SanteDB.Core.Model.Map;
using SanteDB.Core.Model.Query;
using SanteDB.OrmLite.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using SanteDB.Core.Model;
using System.Collections;
using System.Text.RegularExpressions;
using SanteDB.Core.Model.Attributes;
using System.Xml.Serialization;
using SanteDB.OrmLite.Providers;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.DataTypes;

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
        private List<IQueryBuilderHack> m_hacks = new List<IQueryBuilderHack>();

        // Mapper
        private ModelMapper m_mapper;
        private IDbProvider m_provider;

        /// <summary>
        /// Represents model mapper
        /// </summary>
        /// <param name="mapper"></param>
        public QueryBuilder(ModelMapper mapper, IDbProvider provider, params IQueryBuilderHack[] hacks)
        {
            this.m_mapper = mapper;
            this.m_provider = provider;
            this.m_hacks = hacks.ToList();
        }

        /// <summary>
        /// Create a query 
        /// </summary>
        public SqlStatement CreateQuery<TModel>(Expression<Func<TModel, bool>> predicate, params ColumnMapping[] selector)
        {
            var nvc = QueryExpressionBuilder.BuildQuery(predicate, true);
            return CreateQuery<TModel>(nvc, selector);
        }

        /// <summary>
        /// Create query
        /// </summary>
        public SqlStatement CreateQuery<TModel>(IEnumerable<KeyValuePair<String, Object>> query, params ColumnMapping[] selector)
        {
            return CreateQuery<TModel>(query, null, selector);
        }

        /// <summary>
        /// Create query 
        /// </summary>
        public SqlStatement CreateQuery<TModel>(IEnumerable<KeyValuePair<String, Object>> query, String tablePrefix, params ColumnMapping[] selector)
        {
            return CreateQuery<TModel>(query, null, false, selector);
        }

        /// <summary>
        /// Query query 
        /// </summary>
        /// <param name="query"></param>
        public SqlStatement CreateQuery<TModel>(IEnumerable<KeyValuePair<String, Object>> query, String tablePrefix, bool skipJoins, params ColumnMapping[] selector)
        {
            var tableType = m_mapper.MapModelType(typeof(TModel));
            var tableMap = TableMapping.Get(tableType);
            List<TableMapping> scopedTables = new List<TableMapping>() { tableMap };

            bool skipParentJoin = true;
            SqlStatement selectStatement = null;
            KeyValuePair<SqlStatement, List<TableMapping>> cacheHit;

            // Is the query using any of the properties from this table?
            var useKeys = !skipJoins ||
                typeof(IVersionedEntity).IsAssignableFrom(typeof(TModel)) && query.Any(o => {
                    var mPath = this.m_mapper.MapModelProperty(typeof(TModel), typeof(TModel).GetQueryProperty(QueryPredicate.Parse(o.Key).Path));
                    if (mPath == null || mPath.Name == "ObsoletionTime" && o.Value.Equals("null"))
                        return false;
                    else
                        return tableMap.Columns.Any(c => c.SourceProperty == mPath);
                });

            if (skipJoins && !useKeys)
            {
                // If we're skipping joins with a versioned table, then we should really go for the root tablet not the versioned table
                if (typeof(IVersionedEntity).IsAssignableFrom(typeof(TModel)))
                {
                    tableMap = TableMapping.Get(tableMap.Columns.FirstOrDefault(o => o.ForeignKey != null && o.IsAlwaysJoin).ForeignKey.Table);
                    query = query.Where(o => o.Key != "obsoletionTime");
                    scopedTables = new List<TableMapping>() { tableMap };
                }
                selectStatement = new SqlStatement(this.m_provider, $" FROM {tableMap.TableName} AS {tablePrefix}{tableMap.TableName} ");

            }
            else
            {
                if (!s_joinCache.TryGetValue($"{tablePrefix}.{typeof(TModel).Name}", out cacheHit))
                {
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
                    } while (fkStack.Count > 0);

                    // Add the heavy work to the cache
                    lock (s_joinCache)
                        if (!s_joinCache.ContainsKey($"{tablePrefix}.{typeof(TModel).Name}"))
                            s_joinCache.Add($"{tablePrefix}.{typeof(TModel).Name}", new KeyValuePair<SqlStatement, List<TableMapping>>(selectStatement.Build(), scopedTables));
                }
                else
                {
                    selectStatement = cacheHit.Key.Build();
                    scopedTables = cacheHit.Value;
                }
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
                    selectStatement = new SqlStatement(this.m_provider, $"SELECT * ").Append(selectStatement);
                // columnSelector = scopedTables.SelectMany(o => o.Columns).ToArray();
            }
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

            // We want to process each query and build WHERE clauses - these where clauses are based off of the JSON / XML names
            // on the model, so we have to use those for the time being before translating to SQL
            List<KeyValuePair<String, Object>> workingParameters = new List<KeyValuePair<string, object>>(query);

            // Where clause
            SqlStatement whereClause = new SqlStatement(this.m_provider);
            List<SqlStatement> cteStatements = new List<SqlStatement>();

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
                    var subProp = typeof(TModel).GetQueryProperty(propertyPredicate.Path, true);
                    if (subProp == null) throw new MissingMemberException(propertyPredicate.Path);

                    // Link to this table in the other?
                    // Allow hacking of the query before we get to the auto-generated stuff
                    if (!this.m_hacks.Any(o => o.HackQuery(this, selectStatement, whereClause, typeof(TModel), subProp, tablePrefix, propertyPredicate, parm.Value, scopedTables)))
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

                            if (linkColumn == null)
                            {
                                var tableWithJoin = scopedTables.Select(o => o.AssociationWith(subTableMap)).FirstOrDefault(o => o != null);
                                linkColumn = tableWithJoin.Columns.SingleOrDefault(o => scopedTables.Any(s => s.OrmType == o.ForeignKey?.Table));
                                var targetColumn = tableWithJoin.Columns.SingleOrDefault(o => o.ForeignKey.Table == subTableMap.OrmType);
                                subTableColumn = subTableMap.GetColumn(targetColumn.ForeignKey.Column);
                                // The sub-query statement needs to be joined as well 
                                var lnkPfx = IncrementSubQueryAlias(tablePrefix);
                                subQueryStatement.Append($"SELECT {lnkPfx}{tableWithJoin.TableName}.{linkColumn.Name} FROM {tableWithJoin.TableName} AS {lnkPfx}{tableWithJoin.TableName} WHERE ");
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

                                        guardCondition.Append(clsProperty.GetCustomAttributes<XmlElementAttribute>().First().ElementName);
                                        if (typeof(IdentifiedData).IsAssignableFrom(clsModel))
                                            guardCondition.Append(".");
                                    }
                                    subQuery.Add(new KeyValuePair<string, object>(guardCondition.ToString(), guardClause.Key.Split('|')));

                                    // Filter by effective version
                                    if (typeof(IVersionedAssociation).IsAssignableFrom(clsModel))
                                        subQuery.Add(new KeyValuePair<string, object>("obsoleteVersionSequence", new String[] { "null" }));
                                }

                                // Generate method
                                subQuery.RemoveAll(o => String.IsNullOrEmpty(o.Key));
                                var prefix = IncrementSubQueryAlias(tablePrefix);
                                var genMethod = typeof(QueryBuilder).GetGenericMethod("CreateQuery", new Type[] { propertyType }, new Type[] { subQuery.GetType(), typeof(String), typeof(bool), typeof(ColumnMapping[]) });

                                // Sub path is specified
                                if (String.IsNullOrEmpty(propertyPredicate.SubPath) && "null".Equals(parm.Value))
                                    subQueryStatement.And($" NOT EXISTS (");
                                else
                                    subQueryStatement.And($" EXISTS (");

                                if (subQuery.Count(p => !p.Key.Contains(".")) == 0)
                                    subQueryStatement.Append(genMethod.Invoke(this, new Object[] { subQuery, prefix, true, new ColumnMapping[] { subTableColumn } }) as SqlStatement);
                                else
                                    subQueryStatement.Append(genMethod.Invoke(this, new Object[] { subQuery, prefix, false, new ColumnMapping[] { subTableColumn } }) as SqlStatement);

                                subQueryStatement.And($"{existsClause} = {prefix}{subTableMap.TableName}.{subTableColumn.Name}");
                                //existsClause = $"{prefix}{subTableColumn.Table.TableName}.{subTableColumn.Name}";
                                subQueryStatement.Append(")");
                            }

                            if (subTableColumn != linkColumn)
                                whereClause.And($"{tablePrefix}{localTable.TableName}.{localTable.GetColumn(linkColumn.ForeignKey.Column).Name} IN (").Append(subQueryStatement).Append(")");
                            else
                                whereClause.And(subQueryStatement);

                        }
                        else  // this table points at other
                        {
                            var subQuery = queryParms.Select(o => new KeyValuePair<String, Object>(QueryPredicate.Parse(o.Key).ToString(QueryPredicatePart.SubPath), o.Value)).ToList();

                            if (!subQuery.Any(o => o.Key == "obsoletionTime") && typeof(IBaseEntityData).IsAssignableFrom(subProp.PropertyType))
                                subQuery.Add(new KeyValuePair<string, object>("obsoletionTime", "null"));

                            TableMapping tableMapping = null;
                            var subPropKey = typeof(TModel).GetQueryProperty(propertyPredicate.Path);

                            // Get column info
                            PropertyInfo domainProperty = scopedTables.Select(o => { tableMapping = o; return m_mapper.MapModelProperty(typeof(TModel), o.OrmType, subPropKey); })?.FirstOrDefault(o => o != null);
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

                            var fkTableDef = TableMapping.Get(linkColumn.ForeignKey.Table);
                            var fkColumnDef = fkTableDef.GetColumn(linkColumn.ForeignKey.Column);
                            var prefix = IncrementSubQueryAlias(tablePrefix);

                            // Create the sub-query
                            //var genMethod = typeof(QueryBuilder).GetGenericMethod("CreateQuery", new Type[] { subProp.PropertyType }, new Type[] { subQuery.GetType(), typeof(ColumnMapping[]) });
                            //SqlStatement subQueryStatement = genMethod.Invoke(this, new Object[] { subQuery, new ColumnMapping[] { fkColumnDef } }) as SqlStatement;
                            SqlStatement subQueryStatement = null;
                            var subSkipJoins = subQuery.Count(o => !o.Key.Contains(".") && o.Key != "obsoletionTime") == 0;
                            if (String.IsNullOrEmpty(propertyPredicate.CastAs))
                            {
                                var genMethod = typeof(QueryBuilder).GetGenericMethod("CreateQuery", new Type[] { subProp.PropertyType }, new Type[] { subQuery.GetType(), typeof(string), typeof(bool), typeof(ColumnMapping[]) });
                                subQueryStatement = genMethod.Invoke(this, new Object[] { subQuery, prefix, subSkipJoins, new ColumnMapping[] { fkColumnDef } }) as SqlStatement;
                            }
                            else // we need to cast!
                            {
                                var castAsType = new SanteDB.Core.Model.Serialization.ModelSerializationBinder().BindToType("SanteDB.Core.Model", propertyPredicate.CastAs);

                                var genMethod = typeof(QueryBuilder).GetGenericMethod("CreateQuery", new Type[] { castAsType }, new Type[] { subQuery.GetType(), typeof(String), typeof(bool), typeof(ColumnMapping[]) });
                                subQueryStatement = genMethod.Invoke(this, new Object[] { subQuery, prefix, false, new ColumnMapping[] { fkColumnDef } }) as SqlStatement;
                            }

                            //cteStatements.Add(new SqlStatement(this.m_provider, $"{tablePrefix}cte{cteStatements.Count} AS (").Append(subQueryStatement).Append(")"));
                            //subQueryStatement.And($"{tablePrefix}{tableMapping.TableName}.{linkColumn.Name} = {sqName}{fkTableDef.TableName}.{fkColumnDef.Name} ");

                            // Join up to the parent table
                            subQueryStatement.And($"{tablePrefix}{tableMapping.TableName}.{linkColumn.Name} = {prefix}{fkTableDef.TableName}.{fkColumnDef.Name}");

                            whereClause.And($"EXISTS (").Append(subQueryStatement).Append(")");

                        }
                    }
                }
                else if (!this.m_hacks.Any(o => o.HackQuery(this, selectStatement, whereClause, typeof(TModel), typeof(TModel).GetQueryProperty(propertyPredicate.Path), tablePrefix, propertyPredicate, parm.Value, scopedTables)))
                    whereClause.And(CreateWhereCondition(typeof(TModel), propertyPredicate.Path, parm.Value, tablePrefix, scopedTables));

            }

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
        public SqlStatement CreateWhereCondition(Type tmodel, String propertyPath, Object value, String tablePrefix, List<TableMapping> scopedTables)
        {

            // Map the type
            var tableMapping = scopedTables.First();
            var propertyInfo = tmodel.GetQueryProperty(propertyPath);
            if (propertyInfo == null)
                throw new ArgumentOutOfRangeException(propertyPath);
            PropertyInfo domainProperty = scopedTables.Select(o => { tableMapping = o; return m_mapper.MapModelProperty(tmodel, o.OrmType, propertyInfo); }).FirstOrDefault(o => o != null);

            // Now map the property path
            var tableAlias = $"{tablePrefix}{tableMapping.TableName}";
            if (domainProperty == null)
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
        /// <param name="column">The column data for the data model</param>
        /// <param name="values">The values to be matched</param>
        /// <returns></returns>
        public SqlStatement CreateSqlPredicate(String tableAlias, String columnName, PropertyInfo modelProperty, IList values)
        {

            var retVal = new SqlStatement(this.m_provider);

            retVal.Append("(");
            foreach (var itm in values)
            {
                retVal.Append($"{tableAlias}.{columnName}");
                var semantic = " OR ";
                var iValue = itm;
                if (iValue is String)
                {
                    var sValue = itm as String;
                    switch (sValue[0])
                    {
                        case '<':
                            semantic = " AND ";
                            if (sValue[1] == '=')
                                retVal.Append(" <= ?", CreateParameterValue(sValue.Substring(2), modelProperty.PropertyType));
                            else
                                retVal.Append(" < ?", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;
                        case '>':
                            semantic = " AND ";
                            if (sValue[1] == '=')
                                retVal.Append(" >= ?", CreateParameterValue(sValue.Substring(2), modelProperty.PropertyType));
                            else
                                retVal.Append(" > ?", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;
                        case '!':
                            semantic = " AND ";
                            if (sValue.Equals("!null"))
                                retVal.Append(" IS NOT NULL");
                            else
                                retVal.Append(" <> ?", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;
                        case '~':
                            if (sValue.Contains("*") || sValue.Contains("?"))
                                retVal.Append(" ILIKE ? ", CreateParameterValue(sValue.Substring(1).Replace("*", "%"), modelProperty.PropertyType));
                            else
                                retVal.Append(" ILIKE '%' || ? || '%'", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;
                        case '^':
                            retVal.Append(" ILIKE ? || '%'", CreateParameterValue(sValue.Substring(1), modelProperty.PropertyType));
                            break;
                        default:
                            if (sValue.Equals("null"))
                                retVal.Append(" IS NULL");
                            else
                                retVal.Append(" = ? ", CreateParameterValue(sValue, modelProperty.PropertyType));
                            break;
                    }
                }
                else
                    retVal.Append(" = ? ", CreateParameterValue(iValue, modelProperty.PropertyType));

                if (values.IndexOf(itm) < values.Count - 1)
                    retVal.Append(semantic);
            }

            retVal.Append(")");

            return retVal;
        }

        /// <summary>
        /// Create a parameter value
        /// </summary>
        private static object CreateParameterValue(object value, Type toType)
        {
            object retVal = null;
            if (value.GetType() == toType ||
                value.GetType() == toType.StripNullable())
                return value;
            else if (MapUtil.TryConvert(value, toType, out retVal))
                return retVal;
            else
                throw new ArgumentOutOfRangeException(value.ToString());
        }
    }
}
