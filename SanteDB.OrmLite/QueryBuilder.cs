/*
 * Copyright (C) 2021 - 2026, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-6-21
 */
using SanteDB.Core.i18n;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Attributes;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Map;
using SanteDB.Core.Model.Query;
using SanteDB.OrmLite.Attributes;
using SanteDB.OrmLite.Configuration;
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
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        Full = Path | Guard | Cast | SubPath,
        Path = 0x1,
        Guard = 0x2,
        Cast = 0x4,
        SubPath = 0x8,
        UnionWith = 0x10,
        PropertyAndGuard = Path | Guard,
        PropertyAndCast = Path | Cast,
        PropertyAndGuardAndCast = Path | Guard | Cast,
        AllExceptUnion = Path | Guard | Cast | SubPath
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
    }

    /// <summary>
    /// Represents the query predicate
    /// </summary>
    public class QueryPredicate
    {
        /// <summary>
        /// Regex to extract property, guards and cast
        /// </summary>
        public static readonly Regex ExtractionRegex = new Regex(@"^([\$\w]+?)(?:\[([^\]]*?)\])?(?:\@(\w*))?(?:\?)?(?:\.(.*?))?(?:\|\|(.*))?$", RegexOptions.Compiled);

        private const int PropertyRegexGroup = 1;
        private const int GuardRegexGroup = 2;
        private const int CastRegexGroup = 3;
        private const int SubPropertyRegexGroup = 4;
        private const int UnionRegexPropertyGroup = 5;

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
        /// Union with the other property
        /// </summary>
        public QueryPredicate Union { get; private set; }

        /// <summary>
        /// Parse a condition
        /// </summary>
        public static QueryPredicate Parse(String condition)
        {
            var matches = ExtractionRegex.Match(condition);
            if (!matches.Success)
            {
                return null;
            }



            return new QueryPredicate()
            {
                Path = matches.Groups[PropertyRegexGroup].Value,
                CastAs = matches.Groups[CastRegexGroup].Value,
                Guard = Uri.UnescapeDataString(matches.Groups[GuardRegexGroup].Value),
                SubPath = matches.Groups[SubPropertyRegexGroup].Value,
                Union = Parse(matches.Groups[UnionRegexPropertyGroup].Value)
            };
        }

        /// <summary>
        /// Represent the predicate as a string
        /// </summary>
        public String ToString(QueryPredicatePart parts)
        {
            StringBuilder sb = new StringBuilder();

            if ((parts & QueryPredicatePart.Path) != 0)
            {
                sb.Append(this.Path);
            }

            if ((parts & QueryPredicatePart.Guard) != 0 && !String.IsNullOrEmpty(this.Guard))
            {
                sb.AppendFormat("[{0}]", this.Guard);
            }

            if ((parts & QueryPredicatePart.Cast) != 0 && !String.IsNullOrEmpty(this.CastAs))
            {
                sb.AppendFormat("@{0}", this.CastAs);
            }

            if ((parts & QueryPredicatePart.SubPath) != 0 && !String.IsNullOrEmpty(this.SubPath))
            {
                sb.AppendFormat("{0}{1}", sb.Length > 0 ? "." : "", this.SubPath);
            }

            if(this.Union != null && parts.HasFlag(QueryPredicatePart.UnionWith))
            {
                sb.AppendFormat("|{0}", this.Union.ToString());
            }
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

        private readonly String[] NULL_CLAUSE = { "null" };

        // Join cache
        private Dictionary<String, KeyValuePair<SqlStatementBuilder, List<TableMapping>>> s_joinCache = new Dictionary<String, KeyValuePair<SqlStatementBuilder, List<TableMapping>>>();

        // A list of hacks injected into this query builder
        private static List<IQueryBuilderHack> m_hacks = new List<IQueryBuilderHack>();

        // Mapper
        private readonly ModelMapper m_mapper;
        private readonly IDbStatementFactory m_factory;
        private readonly IDbEncryptor m_encryptionProvider;

        /// <summary>
        /// Provider
        /// </summary>
        public IDbStatementFactory StatementFactory => this.m_factory;

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
        public QueryBuilder(ModelMapper mapper, IDbStatementFactory provider)
        {
            this.m_mapper = mapper;
            this.m_factory = provider;
            this.m_encryptionProvider = (provider.Provider as IEncryptedDbProvider)?.GetEncryptionProvider();
        }

        /// <summary>
        /// Create a query
        /// </summary>
        public SqlStatementBuilder CreateWhere<TModel>(Expression<Func<TModel, bool>> predicate)
        {
            var nvc = QueryExpressionBuilder.BuildQuery(predicate, true);
            var tableType = m_mapper.MapModelType(typeof(TModel));
            var tableMap = TableMapping.Get(tableType);
            List<TableMapping> scopedTables = new List<TableMapping>() { tableMap };

            return CreateWhereCondition(typeof(TModel), new SqlStatementBuilder(m_factory), nvc.ToDictionary(), String.Empty, scopedTables, null, out IList<SqlStatementBuilder> _);
        }

        /// <summary>
        /// Create a query from expression without needing type
        /// </summary>
        public SqlStatementBuilder CreateQuery(Type modelType, LambdaExpression predicate, params ColumnMapping[] selector)
        {
            var nvc = QueryExpressionBuilder.BuildQuery(modelType, predicate, true);
            return CreateQuery(modelType, nvc.ToDictionary(), selector);
        }

        /// <summary>
        /// Create a query
        /// </summary>
        public SqlStatementBuilder CreateQuery<TModel>(Expression<Func<TModel, bool>> predicate, params ColumnMapping[] selector)
        {
            var nvc = QueryExpressionBuilder.BuildQuery(predicate);
            return CreateQuery(typeof(TModel), nvc.ToDictionary(), selector);
        }

        /// <summary>
        /// Create query
        /// </summary>
        public SqlStatementBuilder CreateQuery(Type tmodel, IDictionary<String, String[]> query, params ColumnMapping[] selector)
        {
            return CreateQuery(tmodel, query, null, false, null, selector);
        }


        /// <summary>
        /// Query query
        /// </summary>
        /// TODO: Refactor this
        public SqlStatementBuilder CreateQuery(Type tmodel, IDictionary<String, String[]> query, String tablePrefix, bool skipJoins, IEnumerable<TableMapping> parentScopedTables, params ColumnMapping[] selector)
        {
            var tableType = m_mapper.MapModelType(tmodel);
            var tableMap = TableMapping.Get(tableType);
            List<TableMapping> scopedTables = new List<TableMapping>() { tableMap };

            bool skipParentJoin = true;
            SqlStatementBuilder selectStatement = null;
            Dictionary<Type, TableMapping> skippedJoinMappings = new Dictionary<Type, TableMapping>();

            // JF - If there is a disagreement between the claimed foreign key type and the model type 
            //      then we need to join them together
            // Is the query using any of the properties from this table?
            var useKeys = !skipJoins ||
                typeof(IVersionedData).IsAssignableFrom(tmodel) && query.Any(o =>
                {
                    var mPath = this.m_mapper.MapModelProperty(tmodel, tmodel.GetQueryProperty(QueryPredicate.Parse(o.Key).Path));
                    if (mPath == null || mPath.Name == "ObsoletionTime" && o.Value.Equals("null"))
                    {
                        return false;
                    }
                    else
                    {
                        return tableMap.Columns.Any(c => c.SourceProperty == mPath);
                    }
                });

            if (skipJoins && !useKeys)
            {
                // If we're skipping joins with a versioned table, then we should really go for the root tablet not the versioned table
                if (typeof(IVersionedData).IsAssignableFrom(tmodel))
                {
                    tableMap = TableMapping.Get(tableMap.Columns.FirstOrDefault(o => o.ForeignKey != null && o.IsAlwaysJoin).ForeignKey.Table);
                    query.Remove("obsoletionTime");
                    scopedTables = new List<TableMapping>() { tableMap };
                }
                selectStatement = new SqlStatementBuilder(this.m_factory, $" FROM {tableMap.TableName} AS {tablePrefix}{tableMap.TableName} ");
            }
            else
            {
                //if (!s_joinCache.TryGetValue($"{tablePrefix}.{typeof(TModel).Name}", out cacheHit))
                //{
                selectStatement = new SqlStatementBuilder(this.m_factory, $" FROM {tableMap.TableName} AS {tablePrefix}{tableMap.TableName} ");

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
                            for (int i = 0; i < selector.Length; i++)
                            {
                                if (selector[i].Table?.OrmType == fkTbl.OrmType)
                                {
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
                                {
                                    joinFilters.AddRange(flt);
                                }
                                else
                                {
                                    selectStatement.And($"({String.Join(" OR ", flt.Select(o => $"{tablePrefix}{fltCol.Table.TableName}.{fltCol.Name} = '{o.Value}'"))})");
                                    joinFilters.RemoveAll(o => flt.Contains(o));
                                }
                            }

                            selectStatement.Append(")");
                            if (!scopedTables.Contains(fkTbl))
                            {
                                fkStack.Push(fkTbl);
                            }

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
                if (this.m_factory.Features.HasFlag(SqlEngineFeatures.StrictSubQueryColumnNames))
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
                    selectStatement = new SqlStatementBuilder(this.m_factory, $"SELECT {columnList} ").Append(selectStatement);
                }
                else
                {
                    selectStatement = new SqlStatementBuilder(this.m_factory, $"SELECT *").Append(selectStatement);
                }
                // columnSelector = scopedTables.SelectMany(o => o.Columns).ToArray();
            }
            else if (columnSelector.All(o => o.SourceProperty == null)) // Fake / constants
            {
                selectStatement = new SqlStatementBuilder(this.m_factory, $"SELECT {String.Join(",", columnSelector.Select(o => o.Name))} ").Append(selectStatement);
            }
            else
            {
                var columnList = String.Join(",", columnSelector.Select(o =>
                {
                    var rootCol = tableMap.GetColumn(o.SourceProperty);
                    skipParentJoin &= rootCol != null;

                    if (skipParentJoin)
                    {
                        return $"{tablePrefix}{rootCol.Table.TableName}.{rootCol.Name}";
                    }
                    else
                    {
                        // has the column been redirected?
                        var scopedTable = scopedTables.Find(s => s.OrmType == o.Table.OrmType) ?? o.Table;
                        return $"{tablePrefix}{scopedTable.TableName}.{o.Name}";
                    }
                }));
                selectStatement = new SqlStatementBuilder(this.m_factory, $"SELECT {columnList} ").Append(selectStatement);
            }

            var whereClause = this.CreateWhereCondition(tmodel, selectStatement, query, tablePrefix, scopedTables, parentScopedTables, out IList<SqlStatementBuilder> cteStatements);
            // Return statement
            SqlStatementBuilder retVal = new SqlStatementBuilder(this.m_factory)
                .Append(selectStatement)
                .Where(whereClause.Statement);

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
        private SqlStatementBuilder CreateWhereCondition(Type tmodel, SqlStatementBuilder selectStatement, IDictionary<String, String[]> query, string tablePrefix, IEnumerable<TableMapping> scopedTables, IEnumerable<TableMapping> parentScopedTables, out IList<SqlStatementBuilder> cteStatements)
        {
            // We want to process each query and build WHERE clauses - these where clauses are based off of the JSON / XML names
            // on the model, so we have to use those for the time being before translating to SQL
            var workingParameters = query.Where(o => !o.Key.StartsWith("_") || tmodel.GetQueryProperty(o.Key) != null).ToList();

            // Where clause
            SqlStatementBuilder whereClause = new SqlStatementBuilder(this.m_factory);
            cteStatements = new List<SqlStatementBuilder>();

            // Construct
            while (workingParameters.Count > 0)
            {
                var parm = workingParameters.First();
                workingParameters.RemoveAt(0);

                // Match the regex and process
                var key = parm.Key;
                if (String.IsNullOrEmpty(key))
                {
                    key = "id";
                }

                var propertyPredicate = QueryPredicate.Parse(key);
                if (propertyPredicate == null)
                {
                    throw new ArgumentOutOfRangeException(parm.Key);
                }

                // If this is a union with?
                bool shouldUnion = propertyPredicate.Union != null;
                if(shouldUnion)
                {
                    whereClause.And($"( {this.m_factory.CreateSqlKeyword(SqlKeyword.True)} "); // HACK: We add TRUE as our query needs something to AND with
                }

                while (propertyPredicate != null)
                {

                    // Next, we want to construct the other parms
                    var otherParms = workingParameters.Where(o => QueryPredicate.Parse(o.Key).ToString(QueryPredicatePart.PropertyAndGuardAndCast) == propertyPredicate.ToString(QueryPredicatePart.PropertyAndGuardAndCast)).ToArray();

                    // Remove the working parameters if the column is FK then all parameters
                    if (otherParms.Any() || !String.IsNullOrEmpty(propertyPredicate.Guard) || !String.IsNullOrEmpty(propertyPredicate.SubPath))
                    {
                        foreach (var o in otherParms)
                        {
                            workingParameters.Remove(o);
                        }

                        // We need to do a sub query
                        var subQueryParms = new List<KeyValuePair<String, String[]>>() { new KeyValuePair<string, string[]>(propertyPredicate.ToString(QueryPredicatePart.AllExceptUnion), parm.Value) }.Union(otherParms);

                        // Grab the appropriate builder
                        var subProp = tmodel.GetQueryProperty(propertyPredicate.Path, true);
                        if (subProp == null)
                        {
                            throw new MissingMemberException(propertyPredicate.Path);
                        }

                        // Link to this table in the other?
                        // Allow hacking of the query before we get to the auto-generated stuff
                        if (!m_hacks.Any(o => o.HackQuery(this, selectStatement, whereClause, tmodel, subProp, tablePrefix, propertyPredicate, parm.Value, scopedTables, subQueryParms.ToDictionary(q => q.Key, q => q.Value))))
                        {
                            // Is this a collection?
                            if (typeof(IList).IsAssignableFrom(subProp.PropertyType) ||
                                typeof(IEnumerable).IsAssignableFrom(subProp.PropertyType) && subProp.PropertyType.IsGenericType) // Other table points at this on
                            {
                                var propertyType = subProp.PropertyType.StripGeneric();
                                // map and get ORM def'n
                                var subTableType = m_mapper.MapModelType(propertyType);
                                var subTableMap = TableMapping.Get(subTableType);
                                var linkColumns = subTableMap.Columns.Where(o => scopedTables.Any(s => s.OrmType == o.ForeignKey?.Table));

                                //var linkColumn = linkColumns.Count() > 1 ? linkColumns.FirstOrDefault(o=>o.SourceProperty.Name == "SourceKey") : linkColumns.FirstOrDefault();
                                var linkColumn = linkColumns.Count() > 1 ? linkColumns.FirstOrDefault(o => propertyPredicate.SubPath.StartsWith("source") ? o.SourceProperty.Name != "SourceKey" : o.SourceProperty.Name == "SourceKey") : linkColumns.FirstOrDefault();

                                // Link column is null, is there an assoc attrib?
                                SqlStatementBuilder subQueryStatement = new SqlStatementBuilder(this.m_factory);

                                var subTableColumn = linkColumn;
                                string existsClause = String.Empty;
                                var lnkPfx = IncrementSubQueryAlias(tablePrefix);

                                if (linkColumn == null || scopedTables.Any(o => o.AssociationWith(subTableMap) != null)) // Or there is a better linker
                                {
                                    var tableWithJoin = scopedTables.Select(o => o.AssociationWith(subTableMap)).FirstOrDefault(o => o != null);
                                    linkColumn = tableWithJoin.Columns.SingleOrDefault(o => scopedTables.Any(s => s.OrmType == o.ForeignKey?.Table));
                                    var targetColumn = tableWithJoin.Columns.SingleOrDefault(o => o.ForeignKey?.CanQueryFrom(subTableMap.OrmType) == true);
                                    subTableColumn = subTableMap.GetColumn(targetColumn.ForeignKey.Column);
                                    // The sub-query statement needs to be joined as well
                                    subQueryStatement.Append($"SELECT 1 FROM {tableWithJoin.TableName} AS {lnkPfx}{tableWithJoin.TableName} WHERE ");
                                    existsClause = $"{lnkPfx}{tableWithJoin.TableName}.{targetColumn.Name}";
                                    //throw new InvalidOperationException($"Cannot find foreign key reference to table {tableMap.TableName} in {subTableMap.TableName}");
                                }

                                var localTable = scopedTables.Where(o => o.GetColumn(linkColumn.ForeignKey.Column) != null).FirstOrDefault();

                                if (String.IsNullOrEmpty(existsClause))
                                {
                                    existsClause = $"{tablePrefix}{localTable.TableName}.{localTable.GetColumn(linkColumn.ForeignKey.Column).Name}";
                                }

                                var guardConditions = subQueryParms.GroupBy(o => QueryPredicate.Parse(o.Key).Guard);

                                foreach (var guardClause in guardConditions)
                                {
                                    var subQuery = guardClause.Select(o => new KeyValuePair<String, String[]>(QueryPredicate.Parse(o.Key).ToString(QueryPredicatePart.SubPath), o.Value)).ToList();

                                    // TODO: GUARD CONDITION HERE!!!!
                                    // Does the guard clause indicate a complete sub-query?
                                    if (guardClause.Key.Contains("="))
                                    {
                                        var nvc = guardClause.Key.ParseQueryString();
                                        subQuery.AddRange(nvc.ToDictionary());
                                    }
                                    else if (!String.IsNullOrEmpty(guardClause.Key))
                                    {
                                        StringBuilder guardCondition = new StringBuilder();
                                        var clsModel = propertyType;
                                        while (clsModel.GetCustomAttribute<ClassifierAttribute>() != null)
                                        {
                                            var clsProperty = clsModel.GetClassifierProperty();// clsModel.GetRuntimeProperty(clsModel.GetCustomAttribute<ClassifierAttribute>().ClassifierProperty);
                                            clsModel = clsProperty.PropertyType.StripGeneric();
                                            var redirectProperty = clsProperty.GetCustomAttribute<SerializationReferenceAttribute>()?.RedirectProperty;
                                            if (redirectProperty != null)
                                            {
                                                clsProperty = clsProperty.DeclaringType.GetRuntimeProperty(redirectProperty);
                                            }

                                            // Is this a uuid?
                                            guardCondition.Append(clsProperty.GetSerializationName());
                                            if (guardClause.Key.Split('|').All(o => Guid.TryParse(o, out Guid _)))
                                            {
                                                break;
                                            }
                                            else
                                            {
                                                if (typeof(IdentifiedData).IsAssignableFrom(clsModel))
                                                {
                                                    guardCondition.Append(".");
                                                }
                                            }
                                        }
                                        subQuery.Add(new KeyValuePair<string, string[]>(guardCondition.ToString(), guardClause.Key.Split('|')));

                                        // Filter by effective version
                                        if (typeof(IVersionedAssociation).IsAssignableFrom(clsModel))
                                        {
                                            subQuery.Add(new KeyValuePair<string, string[]>("obsoleteVersionSequence", new string[] { "null" }));
                                        }
                                    }

                                    // Generate method
                                    subQuery.RemoveAll(o => String.IsNullOrEmpty(o.Key));
                                    var prefix = IncrementSubQueryAlias(tablePrefix);

                                    // Sub path is specified
                                    if (String.IsNullOrEmpty(propertyPredicate.SubPath) && "null".Equals(parm.Value))
                                    {
                                        subQueryStatement.And($"NOT EXISTS (");
                                    }
                                    // Query Optimization - Sub-Path is specified and the only object is a NOT value (other than classifier)
                                    else if (!String.IsNullOrEmpty(propertyPredicate.SubPath) &&
                                        subQuery.Count <= 2 &&
                                        subQuery.Count(p =>
                                            !p.Key.Contains(".") && (
                                            p.Value.All(v => v.StartsWith("!")) == true)) == 1)
                                    {
                                        subQueryStatement.And($"NOT EXISTS (");
                                        subQuery = subQuery.Select(a => new KeyValuePair<string, string[]>(a.Key, a.Value.Select(v => v.StartsWith("!") ? v.Substring(1) : v).ToArray())).ToList();
                                    }
                                    else
                                    {
                                        subQueryStatement.And($"EXISTS (");
                                    }

                                    // Does this query object have obsolete version sequence?
                                    if (typeof(IVersionedAssociation).IsAssignableFrom(propertyType)) // Add obslt guard
                                    {
                                        subQuery.Add(new KeyValuePair<string, string[]>(propertyType.GetRuntimeProperty(nameof(IVersionedAssociation.ObsoleteVersionSequenceId)).GetSerializationName(), NULL_CLAUSE));
                                    }

                                    var genMethod = typeof(QueryBuilder).GetGenericMethod("CreateQuery", new Type[] { propertyType }, new Type[] { subQuery.GetType(), typeof(String), typeof(bool), typeof(List<TableMapping>), typeof(ModelSort<>).MakeGenericType(propertyType).MakeArrayType(), typeof(ColumnMapping[]) });

                                    if (subQuery.Count(p => !p.Key.Contains(".")) == 0)
                                    {
                                        subQueryStatement.Append(this.CreateQuery(propertyType, subQuery.ToParameterDictionary(), prefix, true, scopedTables, new ColumnMapping[] { ColumnMapping.One }));
                                    }
                                    else
                                    {
                                        subQueryStatement.Append(this.CreateQuery(propertyType, subQuery.ToParameterDictionary(), prefix, false, scopedTables, new ColumnMapping[] { ColumnMapping.One }));
                                    }

                                    subQueryStatement.And($"{existsClause} = {prefix}{subTableMap.TableName}.{subTableColumn.Name}");
                                    //existsClause = $"{prefix}{subTableColumn.Table.TableName}.{subTableColumn.Name}";

                                    subQueryStatement.Append(")");
                                }

                                if (subTableColumn != linkColumn)
                                {
                                    whereClause.And($"EXISTS (").Append(subQueryStatement).And($"{tablePrefix}{localTable.TableName}.{localTable.GetColumn(linkColumn.ForeignKey.Column).Name} = {lnkPfx}{linkColumn.Table.TableName}.{linkColumn.Name}").Append(")");
                                }
                                else
                                {
                                    whereClause.And(subQueryStatement.Statement);
                                }
                            }
                            else  // this table points at other
                            {
                                var subQuery = subQueryParms.Select(o => new KeyValuePair<String, String[]>(QueryPredicate.Parse(o.Key).ToString(QueryPredicatePart.SubPath), o.Value)).ToList();

                                if (!subQuery.Any(o => o.Key == "obsoletionTime") && typeof(IBaseData).IsAssignableFrom(subProp.PropertyType))
                                {
                                    subQuery.Add(new KeyValuePair<string, string[]>("obsoletionTime", NULL_CLAUSE));
                                }

                                TableMapping tableMapping = null;
                                var subPropKey = tmodel.GetQueryProperty(propertyPredicate.Path);

                                // Get column info
                                PropertyInfo domainProperty = scopedTables.Select(o => { tableMapping = o; return m_mapper.MapModelProperty(tmodel, o.OrmType, subPropKey); })?.FirstOrDefault(o => o != null);
                                ColumnMapping linkColumn = null;
                                // If the domain property is not set, we may have to infer the link
                                if (domainProperty == null)
                                {
                                    var subPropType = m_mapper.MapModelType(subProp.PropertyType.StripGeneric());
                                    // We find the first column with a foreign key that points to the other !!!
                                    linkColumn = scopedTables.SelectMany(o => o.Columns).FirstOrDefault(o => o.ForeignKey?.Table == subPropType);

                                }
                                else
                                {
                                    linkColumn = tableMapping.GetColumn(domainProperty);
                                }

                                var fkTableDef = parentScopedTables?.FirstOrDefault(o => o.OrmType == linkColumn.ForeignKey.Table) ?? TableMapping.Get(linkColumn.ForeignKey.Table);
                                var fkColumnDef = fkTableDef.GetColumn(linkColumn.ForeignKey.Column);
                                var prefix = IncrementSubQueryAlias(tablePrefix);

                                // Create the sub-query
                                //var genMethod = typeof(QueryBuilder).GetGenericMethod("CreateQuery", new Type[] { subProp.PropertyType }, new Type[] { subQuery.GetType(), typeof(ColumnMapping[]) });
                                //SqlStatement subQueryStatement = genMethod.Invoke(this, new Object[] { subQuery, new ColumnMapping[] { fkColumnDef } }) as SqlStatement;
                                SqlStatementBuilder subQueryStatement = null;
                                var fkTypeDisagreement = fkTableDef.OrmType != this.m_mapper.MapModelType(subProp.PropertyType);
                                var subSkipJoins = subQuery.Count(o => !o.Key.Contains(".") && o.Key != "obsoletionTime") == 0 && !fkTypeDisagreement;
                                if (String.IsNullOrEmpty(propertyPredicate.CastAs))
                                {
                                    subQueryStatement = this.CreateQuery(subProp.PropertyType, subQuery.ToParameterDictionary(), prefix, subSkipJoins, scopedTables, new ColumnMapping[] { fkColumnDef });
                                }
                                else // we need to cast!
                                {
                                    var castAsType = new SanteDB.Core.Model.Serialization.ModelSerializationBinder().BindToType("SanteDB.Core.Model", propertyPredicate.CastAs);
                                    subQueryStatement = this.CreateQuery(castAsType, subQuery.ToParameterDictionary(), prefix, false, scopedTables, new ColumnMapping[] { fkColumnDef });
                                }

                                //cteStatements.Add(new SqlStatement(this.m_provider, $"{tablePrefix}cte{cteStatements.Count} AS (").Append(subQueryStatement).Append(")"));
                                //subQueryStatement.And($"{tablePrefix}{tableMapping.TableName}.{linkColumn.Name} = {sqName}{fkTableDef.TableName}.{fkColumnDef.Name} ");

                                // Join up to the parent table

                                whereClause.And($" {tablePrefix}{tableMapping.TableName}.{linkColumn.Name} IN (").Append(subQueryStatement).Append(")");
                            }
                        }
                    }
                    else if (!m_hacks.Any(o => o.HackQuery(this, selectStatement, whereClause, tmodel, tmodel.GetQueryProperty(propertyPredicate.Path), tablePrefix, propertyPredicate, parm.Value, scopedTables, query)))
                    {
                        whereClause.And(CreateWhereCondition(tmodel, propertyPredicate.Path, parm.Value, tablePrefix, scopedTables));
                    }

                    if(shouldUnion)
                    {
                        whereClause.Append(" OR ");
                    }
                    propertyPredicate = propertyPredicate.Union;
                }

                if(shouldUnion)
                {
                    whereClause.RemoveLast(out _).Append(")");
                }
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
        /// <param name="order">Whether to order by ascending or descending.</param>
        private SqlStatementBuilder CreateOrderBy(Type tmodel, string tablePrefix, IEnumerable<TableMapping> scopedTables, Expression sortExpression, SortOrderType order)
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
                    {
                        throw new InvalidOperationException("OrderBy can only be performed on primary properties of the object");
                    }

                    // Determine the map
                    var tableMapping = scopedTables.First();
                    var propertyInfo = mexpr.Member as PropertyInfo;
                    PropertyInfo domainProperty = scopedTables.Select(o => { tableMapping = o; return m_mapper.MapModelProperty(tmodel, o.OrmType, propertyInfo); }).FirstOrDefault(o => o != null);
                    var columnData = tableMapping.GetColumn(domainProperty);
                    return new SqlStatementBuilder(this.m_factory, $" {columnData.Name} {(order == SortOrderType.OrderBy ? "ASC" : "DESC")}");

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
            {
                return "sq0";
            }
            else
            {
                int sq = 0;
                if (Int32.TryParse(tablePrefix.Substring(2), out sq))
                {
                    return "sq" + (sq + 1);
                }
                else
                {
                    return "sq0";
                }
            }
        }

        /// <summary>
        /// Create a where condition
        /// </summary>
        public SqlStatement CreateWhereCondition(Type tmodel, String propertyPath, Object value, String tablePrefix, IEnumerable<TableMapping> scopedTables)
        {
            // Map the type
            var tableMapping = scopedTables.First();
            PropertyInfo propertyInfo = null;
            if (propertyPath == "$self")
            {
                propertyInfo = tableMapping.PrimaryKey.First().SourceProperty;
            }
            else
            {
                propertyInfo = tmodel.GetQueryProperty(propertyPath);

            }
            if (propertyInfo == null)
            {
                throw new ArgumentOutOfRangeException(propertyPath);
            }

            PropertyInfo domainProperty = scopedTables.Select(o => { tableMapping = o; return m_mapper.MapModelProperty(tmodel, o.OrmType, propertyInfo); }).FirstOrDefault(o => o != null);

            // Now map the property path
            var tableAlias = $"{tablePrefix}{tableMapping.TableName}";
            Guid pkey = Guid.Empty;

            var sValue = value.ToString();
            if (value is IList)
            {
                var vals = (value as IEnumerable).OfType<Object>().Where(s => !"!null".Equals(s));
                if (vals.Any())
                {
                    sValue = vals.First().ToString();
                }
            }

            if (domainProperty == null && Guid.TryParse(sValue, out pkey))
            {
                domainProperty = tableMapping.PrimaryKey.First().SourceProperty;
                // Link property to the key
                propertyInfo = tmodel.GetProperty(propertyInfo.Name + "Key");
            }
            else if (domainProperty == null)
            {
                throw new ArgumentException($"Can't find SQL based property for {propertyPath} on {tableMapping.TableName}");
            }

            var columnData = tableMapping.GetColumn(domainProperty);

            // List of parameters
            var lValue = value as IList;
            if (lValue == null)
            {
                lValue = new List<Object>() { value };
            }

            return CreateSqlPredicate(tableAlias, columnData, columnData.SourceProperty, lValue);
        }

        /// <summary>
        /// Create the actual SQL predicate
        /// </summary>
        /// <param name="tableAlias">The alias for the table on which the predicate is based</param>
        /// <param name="domainProperty">The model property information for type information</param>
        /// <param name="columnMapping">The column data for the data model</param>
        /// <param name="values">The values to be matched</param>
        public SqlStatement CreateSqlPredicate(String tableAlias, ColumnMapping columnMapping, PropertyInfo domainProperty, IList values)
        {
            if (domainProperty == null)
            {
                throw new ArgumentNullException(nameof(domainProperty));
            }

            var retVal = new SqlStatementBuilder(this.m_factory);

            bool noCase = domainProperty.GetCustomAttribute<IgnoreCaseAttribute>() != null;
            retVal.Append("(");
            for (var i = 0; i < values.Count; i++)
            {
                var itm = values[i];


                if (noCase)
                {
                    retVal.Append($"{this.m_factory.CreateSqlKeyword(SqlKeyword.Lower)}({tableAlias}.{columnMapping.Name})");
                }
                else
                {
                    retVal.Append($"{tableAlias}.{columnMapping.Name}");
                }

                var semantic = " OR ";

                OrmAleMode aleMode = OrmAleMode.Off;
                var isEncrypted = this.m_encryptionProvider?.TryGetEncryptionMode(columnMapping.EncryptedColumnId, out aleMode) == true &&
                    aleMode != OrmAleMode.Off;
                object eValue = null;
                if (isEncrypted)
                {
                    eValue = this.m_encryptionProvider.CreateQueryValue(aleMode, itm);
                }

                if (itm is String sValue)
                {
                    sValue = noCase ? sValue.ToLowerInvariant() : sValue;
                    switch (sValue[0])
                    {
                        case ':': // function
                            if (isEncrypted)
                            {
                                throw new NotSupportedException(ErrorMessages.FILTER_ENCRYPTED_FIELD);
                            }
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
                                    var pv = parmExtract.Groups[1].Value;
                                    if (pv.StartsWith("\"") && pv.EndsWith("\""))
                                    {
                                        pv = pv.Substring(1, pv.Length - 2);
                                    }

                                    if (!String.IsNullOrEmpty(pv))
                                    {
                                        extendedParms.Add(pv);
                                    }
                                    parmExtract = QueryFilterExtensions.ParameterExtractRegex.Match(parmExtract.Groups[2].Value);
                                }
                                // Now find the function
                                var filterFn = this.m_factory.GetFilterFunction(fnName);
                                if (filterFn == null)
                                {
                                    throw new MissingMethodException(String.Format(ErrorMessages.METHOD_NOT_FOUND, fnName));
                                }
                                else
                                {
                                    retVal = filterFn.CreateSqlStatement(retVal.RemoveLast(out _), $"{tableAlias}.{columnMapping.Name}", extendedParms.ToArray(), operand, domainProperty.PropertyType);
                                }
                            }
                            else
                            {
                                retVal.Append($" = ? ", CreateParameterValue(sValue, domainProperty.PropertyType));
                            }

                            break;

                        case '<':
                            if (isEncrypted)
                            {
                                throw new NotSupportedException(ErrorMessages.FILTER_ENCRYPTED_FIELD);
                            }

                            semantic = " AND ";
                            if (sValue[1] == '=')
                            {
                                retVal.Append($" <= ?", CreateParameterValue(sValue.Substring(2), domainProperty.PropertyType));
                            }
                            else
                            {
                                retVal.Append($" < ?", CreateParameterValue(sValue.Substring(1), domainProperty.PropertyType));
                            }

                            break;

                        case '>':
                            if (isEncrypted)
                            {
                                throw new NotSupportedException(ErrorMessages.FILTER_ENCRYPTED_FIELD);
                            }

                            // peek the next value and see if it is < then we use BETWEEN
                            if (i < values.Count - 1 && values[i + 1].ToString().StartsWith("<"))
                            {
                                object lower = null, upper = null;
                                if (sValue[1] == '=')
                                {
                                    lower = CreateParameterValue(sValue.Substring(2), domainProperty.PropertyType);
                                }
                                else
                                {
                                    lower = CreateParameterValue(sValue.Substring(1), domainProperty.PropertyType);
                                }

                                sValue = values[++i].ToString();
                                if (sValue[1] == '=')
                                {
                                    upper = CreateParameterValue(sValue.Substring(2), domainProperty.PropertyType);
                                }
                                else
                                {
                                    upper = CreateParameterValue(sValue.Substring(1), domainProperty.PropertyType);
                                }

                                semantic = " OR ";
                                retVal.Append($" BETWEEN ? AND ?", lower, upper);
                            }
                            else
                            {
                                semantic = " AND ";
                                if (sValue[1] == '=')
                                {
                                    retVal.Append($" >= ?", CreateParameterValue(sValue.Substring(2), domainProperty.PropertyType));
                                }
                                else
                                {
                                    retVal.Append($" > ?", CreateParameterValue(sValue.Substring(1), domainProperty.PropertyType));
                                }
                            }
                            break;

                        case '!':
                            semantic = " AND ";
                            if (itm.Equals("!null"))
                            {
                                retVal.Append(" IS NOT NULL");
                            }
                            else
                            {
                                retVal.Append($" <> ?", CreateParameterValue(isEncrypted ? eValue : sValue.Substring(1), domainProperty.PropertyType));
                            }

                            break;

                        case '~':
                            if (isEncrypted)
                            {
                                throw new NotSupportedException(ErrorMessages.FILTER_ENCRYPTED_FIELD);
                            }

                            retVal.Append($" {this.m_factory.CreateSqlKeyword(SqlKeyword.ILike)} ? ", CreateParameterValue($"%{sValue.Substring(1)}%", domainProperty.PropertyType));
                            break;

                        case '^':
                            if (isEncrypted)
                            {
                                throw new NotSupportedException(ErrorMessages.FILTER_ENCRYPTED_FIELD);
                            }

                            retVal.Append($" {this.m_factory.CreateSqlKeyword(SqlKeyword.ILike)} ? ", CreateParameterValue($"{sValue.Substring(1)}%", domainProperty.PropertyType));
                            break;

                        case '$':
                            if (isEncrypted)
                            {
                                throw new NotSupportedException(ErrorMessages.FILTER_ENCRYPTED_FIELD);
                            }

                            retVal.Append($" {this.m_factory.CreateSqlKeyword(SqlKeyword.ILike)} ?", CreateParameterValue($"%{sValue.Substring(1)}", domainProperty.PropertyType));
                            break;

                        default:
                            if (itm.Equals("null"))
                            {
                                retVal.Append(" IS NULL");
                            }
                            else
                            {
                                retVal.Append($" = ? ", CreateParameterValue(isEncrypted ? eValue : sValue, domainProperty.PropertyType));
                            }

                            break;
                    }
                }
                else
                {
                    retVal.Append($" = ? ", CreateParameterValue(itm, domainProperty.PropertyType));
                }

                if (i < values.Count - 1)
                {
                    retVal.Append(semantic);
                }
            }

            retVal.Append(")");

            return retVal.Statement;
        }


        /// <summary>
        /// Create parameter value
        /// </summary>
        public static object CreateParameterValue(object value, Type propertyType)
        {
            if (value is String str)
            {
                if (str.Length > 1 && str.StartsWith("\"") && str.EndsWith(("\"")))
                {
                    value = str.Substring(1, str.Length - 2).Replace("\\\"", "\"");
                }
                else if (str.Equals("null", StringComparison.OrdinalIgnoreCase))
                {
                    return DBNull.Value;
                }
                else if (propertyType.StripNullable().Equals(typeof(Guid)) && Guid.TryParse(str, out var uuid))
                {
                    return uuid;
                }

            }

            if (value.GetType() == propertyType ||
                value.GetType() == propertyType.StripNullable())
            {
                return value;
            }
            else if (MapUtil.TryConvert(value, propertyType, out var retVal))
            {
                return retVal;
            }
            else
            {
                throw new ArgumentOutOfRangeException(value.ToString());
            }
        }
    }
}