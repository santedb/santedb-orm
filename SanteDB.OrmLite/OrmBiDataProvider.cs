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
using DocumentFormat.OpenXml.Drawing;
using SanteDB.BI;
using SanteDB.BI.Model;
using SanteDB.BI.Services;
using SanteDB.BI.Services.Impl;
using SanteDB.BI.Util;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Exceptions;
using SanteDB.Core.i18n;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Configuration;
using SanteDB.OrmLite.Providers;
using SharpCompress;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// ADO.NET BIS Data Source
    /// </summary>
    public class OrmBiDataProvider : IBiDataSource
    {
        // Tracer
        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(OrmBiDataProvider));

        // Parameter regular expression
        private static readonly Regex m_parmRegex = new Regex(@"\$\{([\w_][\-\d\w\._]*?)\}", RegexOptions.Compiled);

        // Services
        private readonly IPolicyEnforcementService m_policyEnforcementService;
        private readonly IConfigurationManager m_configurationManager;
        private readonly IBiMetadataRepository m_metadataRepository;
        private readonly IBiPivotProvider m_pivotProvider;

        /// <summary>
        /// DI constructor
        /// </summary>
        public OrmBiDataProvider(IPolicyEnforcementService policyEnforcementService, IConfigurationManager configurationManager, IBiMetadataRepository biMetadataRepository = null, IBiPivotProvider biPivotProvider = null)
        {
            this.m_policyEnforcementService = policyEnforcementService;
            this.m_configurationManager = configurationManager;
            this.m_metadataRepository = biMetadataRepository;
            this.m_pivotProvider = biPivotProvider ?? new InMemoryPivotProvider();
        }
        /// <summary>
        /// Create materialized view
        /// </summary>
        /// <param name="materializeDefinition">The materialized query definition</param>
        public void CreateMaterializedView(BiQueryDefinition materializeDefinition)
        {
            if (materializeDefinition == null)
            {
                throw new ArgumentNullException(nameof(materializeDefinition));
            }

            materializeDefinition = BiUtils.ResolveRefs(materializeDefinition);

            // The ADO.NET provider only allows one connection to one db at a time, so verify the connections are appropriate
            if (materializeDefinition.DataSources?.Count != 1)
            {
                throw new InvalidOperationException($"ADO.NET BI queries can only source data from 1 connection source, query {materializeDefinition.Name} has {materializeDefinition.DataSources?.Count}");
            }

            // We want to open the specified connection
            var provider = this.GetProvider(materializeDefinition);

            // Query definition
            var rdbmsQueryDefinition = this.GetSqlDefinition(materializeDefinition, provider);

            if (rdbmsQueryDefinition.Materialize == null)
            {
                return; // no materialized view
            }
            else if (String.IsNullOrEmpty(rdbmsQueryDefinition.Materialize.Name))
            {
                throw new InvalidOperationException($"Materialization on {materializeDefinition.Id} must have a unique name");
            }
            else if (m_parmRegex.IsMatch(rdbmsQueryDefinition.Materialize.Sql))
            {
                throw new InvalidOperationException("Materializations are not allowed to have parameters references - move parameters to the SQL definition");
            }

            // Get connection and execute
            using (var context = provider.GetWriteConnection())
            {
                try
                {
                    context.Open();
                    context.CommandTimeout = 360000;
                    var sql = new SqlStatement(provider.StatementFactory.CreateSqlKeyword(SqlKeyword.CreateMaterializedView)) + rdbmsQueryDefinition.Materialize.Name
                        + " AS "
                        + rdbmsQueryDefinition.Materialize.Sql;
                    context.ExecuteNonQuery(sql);
                }
                catch (Exception e)
                {
                    throw new DataPersistenceException($"Error creating materialized view for {materializeDefinition.Id}", e);
                }
            }
        }

        /// <summary>
        /// Prepare the query statement
        /// </summary>
        /// <param name="queryDefinition">The SQL query definition to prepare</param>
        /// <param name="parameters">The parameters to pass to the query</param>
        /// <returns>The constructed SqlStatement</returns>
        private SqlStatement PrepareQueryStatement(BiQueryDefinition queryDefinition, IDictionary<String, object> parameters)
        {
            if (queryDefinition == null)
            {
                throw new ArgumentNullException(nameof(queryDefinition));
            }

            queryDefinition = BiUtils.ResolveRefs(queryDefinition);
            // The ADO.NET provider only allows one connection to one db at a time, so verify the connections are appropriate
            if (queryDefinition.DataSources?.Count != 1)
            {
                throw new InvalidOperationException($"ADO.NET BI queries can only source data from 1 connection source, query {queryDefinition.Name} has {queryDefinition.DataSources?.Count}");
            }

            // Ensure we have sufficient priviledge
            this.AclCheck(queryDefinition);

            // Apply defaults where possible
            foreach (var defaultParm in queryDefinition.Parameters.Where(p => !String.IsNullOrEmpty(p.DefaultValue) && !parameters.ContainsKey(p.Name)))
            {
                parameters.Add(defaultParm.Name, defaultParm.DefaultValue);
            }

            // Next we validate parameters
            if (!queryDefinition.Parameters.Where(p => p.Required == true).All(p => parameters.ContainsKey(p.Name)))
            {
                throw new InvalidOperationException("Missing required parameter");
            }

            // Validate parameter values
            foreach (var kv in parameters.ToArray())
            {
                var parmDef = queryDefinition.Parameters.FirstOrDefault(p => p.Name == kv.Key);
                if (parmDef == null)
                {
                    continue; // skip
                }
                else
                {
                    switch (parmDef.Type)
                    {
                        case BiDataType.Boolean:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                            {
                                parameters[kv.Key] = DBNull.Value;
                            }
                            else
                            {
                                parameters[kv.Key] = Boolean.Parse(kv.Value.ToString());
                            }

                            break;

                        case BiDataType.Date:
                        case BiDataType.DateTime:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                            {
                                parameters[kv.Key] = DBNull.Value;
                            }
                            else
                            {
                                parameters[kv.Key] = DateTime.Parse(kv.Value.ToString());
                            }

                            break;

                        case BiDataType.Integer:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                            {
                                parameters[kv.Key] = DBNull.Value;
                            }
                            else
                            {
                                parameters[kv.Key] = Int32.Parse(kv.Value.ToString());
                            }

                            break;

                        case BiDataType.String:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                            {
                                parameters[kv.Key] = DBNull.Value;
                            }
                            else
                            {
                                parameters[kv.Key] = kv.Value.ToString();
                            }

                            break;

                        case BiDataType.Uuid:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                            {
                                parameters[kv.Key] = DBNull.Value;
                            }
                            else
                            {
                                parameters[kv.Key] = Guid.Parse(kv.Value.ToString());
                            }

                            break;

                        default:
                            throw new InvalidOperationException($"Cannot determine how to parse {parmDef.Type}");
                    }
                }
            }

            // We want to open the specified connection
            var provider = this.GetProvider(queryDefinition);

            // Query definition
            var rdbmsQueryDefinition = this.GetSqlDefinition(queryDefinition, provider);

            // Prepare the templated SQL
            List<Object> values = new List<object>();
            var stmt = m_parmRegex.Replace(rdbmsQueryDefinition.Sql, (m) =>
            {
                object pValue = null;
                parameters.TryGetValue(m.Groups[1].Value, out pValue);
                values.Add(pValue);
                return "?";
            });

            var sqlStatement = new SqlStatement(stmt, values.ToArray()).Prepare();

            if(queryDefinition.WithQuery?.Any() == true)
            {
                var withStatements = queryDefinition.WithQuery.ToDictionary(o => o.Name, o => this.PrepareQueryStatement(o, parameters));
                var withStmt = new SqlStatement("WITH ");
                foreach(var kv in withStatements)
                {
                    withStmt = withStmt.Append($" {kv.Key} AS (").Append(kv.Value).Append(") ").Append(",");
                }
                withStmt = withStmt.RemoveLast(out _);
                sqlStatement = withStmt.Append(sqlStatement);
            }
            return sqlStatement;
        }

        /// <summary>
        /// Executes the query
        /// </summary>
        public BisResultContext ExecuteQuery(BiQueryDefinition queryDefinition, IDictionary<string, object> parameters, BiAggregationDefinition[] aggregation, int? offset = null, int? count = null)
        {
            var sqlStmt = this.PrepareQueryStatement(queryDefinition, parameters);

            // We want to open the specified connection
            var provider = this.GetProvider(queryDefinition);

            // Aggregation definitions
            if (aggregation?.Length > 0)
            {
                var agg = aggregation.FirstOrDefault(o => o.Invariants?.Contains(provider.Invariant) == true) ??
                    aggregation.FirstOrDefault(o => o.Invariants?.Count == 0) ??
                    aggregation.FirstOrDefault(o => o.Invariants == null);

                // Aggregation found
                if (agg == null)
                {
                    throw new InvalidOperationException($"No provided aggregation can be found for {provider.Invariant}");
                }

                var selector = agg.Columns?.Select(o=>this.PrepareAggregateFunction(o)).ToArray() ?? new string[] { "*" };
                String[] groupings = agg.Groupings.Select(g => g.ColumnSelector).ToArray(),
                    colGroupings = agg.Groupings.Select(g => $"{g.ColumnSelector} AS {g.Name}").ToArray();
                // Aggregate

                sqlStmt = new SqlStatement($"SELECT {String.Join(",", colGroupings.Concat(selector))} FROM (").Append(sqlStmt).Append(") AS _inner ")
                    .Append($" GROUP BY {String.Join(",", groupings)}");

                if (agg.Sorting?.Any() == true)
                {
                    sqlStmt = sqlStmt.Append($" ORDER BY {String.Join(",", agg.Sorting.Select(o => $"{(o.Name ?? o.ColumnSelector)} {(o.Direction == BiOrderColumnDirection.Ascending ? "asc" : "desc")}"))}");
                }
            }

            // Get a readonly context
            try
            {
                DateTime startTime = DateTime.Now;
                this.m_tracer.TraceInfo("Executing BI Query: {0}", sqlStmt.ToString());
                var results = new OrmResultSet<ExpandoObject>(provider.GetReadonlyConnection(), sqlStmt);
                if (offset.HasValue)
                {
                    results = results.Skip(offset.Value);
                }
                if (count.HasValue)
                {
                    results = results.Take(count.Value);
                }
                return new BisResultContext(
                    queryDefinition,
                    parameters,
                    this,
                    new OrmQueryResultSet<ExpandoObject>(results),
                    startTime);
            }
            catch (Exception e)
            {
                this.m_tracer.TraceError("Error executing BIS data query {1} \r\n SQL: {2}\r\n Error: {0}", e, queryDefinition.Id, sqlStmt);
                throw new DataPersistenceException($"Error executing BIS data query", e);
            }
        }

        /// <summary>
        /// Prepare the aggregate function
        /// </summary>
        private String PrepareAggregateFunction(BiAggregateSqlColumnReference columnRef, String defaultName = null)
        {
            var sb = new StringBuilder();
            switch (columnRef.Aggregation)
            {
                case BiAggregateFunction.Average:
                    sb.Append($"AVG({columnRef.ColumnSelector})");
                    break;
                case BiAggregateFunction.Count:
                    sb.Append($"COUNT({columnRef.ColumnSelector})");
                    break;

                case BiAggregateFunction.CountDistinct:
                    sb.Append($"COUNT(DISTINCT {columnRef.ColumnSelector})");
                    break;

                case BiAggregateFunction.First:
                    sb.Append($"FIRST({columnRef.ColumnSelector})");
                    break;

                case BiAggregateFunction.Last:
                    sb.Append($"LAST({columnRef.ColumnSelector})");
                    break;

                case BiAggregateFunction.Max:
                    sb.Append($"MAX({columnRef.ColumnSelector})");
                    break;

                case BiAggregateFunction.Min:
                    sb.Append($"MIN({columnRef.ColumnSelector})");
                    break;

                case BiAggregateFunction.Sum:
                    sb.Append($"SUM({columnRef.ColumnSelector})");
                    break;

                case BiAggregateFunction.Value:
                    sb.Append($"{columnRef.ColumnSelector}");
                    break;

                default:
                    throw new InvalidOperationException("Cannot apply aggregation function");
            }

            var colName = columnRef.Name ?? defaultName;
            if(!String.IsNullOrEmpty(colName))
            {
                sb.Append($" AS {colName} ");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Get the SQL definition for the specified provider invariant
        /// </summary>
        /// <param name="queryDefinition">The query definition from which the SQL should be extracted</param>
        /// <param name="provider">The provider for which the SQL should be retrieved</param>
        /// <returns>The SQL definition</returns>
        private BiSqlDefinition GetSqlDefinition(BiQueryDefinition queryDefinition, IDbProvider provider)
        {
            var rdbmsQueryDefinition = queryDefinition.QueryDefinitions.FirstOrDefault(o => o.Invariants.Contains(provider.Invariant));
            if (rdbmsQueryDefinition == null)
            {
                throw new InvalidOperationException($"Could not find a SQL definition for invariant {provider.Invariant} from {queryDefinition?.Id} (supported invariants: {String.Join(",", queryDefinition.QueryDefinitions.SelectMany(o => o.Invariants))})");
            }

            return rdbmsQueryDefinition;
        }

        /// <summary>
        /// Get provider
        /// </summary>
        private IDbProvider GetProvider(BiQueryDefinition queryDefinition)
        {
            var dataSource = queryDefinition.DataSources.First();
            var connectionString = this.m_configurationManager.GetConnectionString(dataSource.ConnectionString ?? dataSource.Id);
            var provider = this.m_configurationManager.GetSection<OrmConfigurationSection>().GetProvider(connectionString.Provider);
            provider.ConnectionString = connectionString.Value;
            provider.ReadonlyConnectionString = connectionString.Value;
            return provider;
        }

        /// <summary>
        /// Perform a check on the ACL for the 
        /// </summary>
        /// <param name="queryDefinition">The query definition to perform a demand on</param>
        private void AclCheck(BiQueryDefinition queryDefinition)
        {

            var demandList = queryDefinition.DataSources.SelectMany(o => o?.MetaData.Demands);
            if (queryDefinition.MetaData?.Demands != null)
            {
                demandList = demandList.Union(queryDefinition.MetaData?.Demands);
            }

            foreach (var pol in demandList)
            {
                this.m_policyEnforcementService.Demand(pol);
            }
        }

        /// <summary>
        /// Execute the specified query
        /// </summary>
        public BisResultContext ExecuteQuery(string queryId, IDictionary<string, object> parameters, BiAggregationDefinition[] aggregation, int? offset, int? count)
        {
            var query = this.m_metadataRepository?.Get<BiQueryDefinition>(queryId);
            if (query == null)
            {
                throw new KeyNotFoundException(queryId);
            }
            else
            {
                return this.ExecuteQuery(query, parameters, aggregation, offset, count);
            }
        }

        /// <summary>
        /// Executes the specified view
        /// </summary>
        public BisResultContext ExecuteView(BiViewDefinition viewDef, IDictionary<string, object> parameters, int? offset = null, int? count = null)
        {
            viewDef = BiUtils.ResolveRefs(viewDef) as BiViewDefinition;
            var retVal = this.ExecuteQuery(viewDef.Query, parameters, viewDef.AggregationDefinitions?.ToArray(), offset, count);
            if (viewDef.Pivot != null)
            {
                retVal = this.m_pivotProvider.Pivot(retVal, viewDef.Pivot);
            }

            return retVal;
        }

        /// <summary>
        /// Refresh materialized view
        /// </summary>
        public void RefreshMaterializedView(BiQueryDefinition materializeDefinition)
        {
            if (materializeDefinition == null)
            {
                throw new ArgumentNullException(nameof(materializeDefinition));
            }

            materializeDefinition = BiUtils.ResolveRefs(materializeDefinition);
            // The ADO.NET provider only allows one connection to one db at a time, so verify the connections are appropriate
            if (materializeDefinition.DataSources?.Count != 1)
            {
                throw new InvalidOperationException($"ADO.NET BI queries can only source data from 1 connection source, query {materializeDefinition.Name} has {materializeDefinition.DataSources?.Count}");
            }

            // We want to open the specified connection
            var provider = this.GetProvider(materializeDefinition);

            // Query definition
            var rdbmsQueryDefinition = this.GetSqlDefinition(materializeDefinition, provider);

            if (rdbmsQueryDefinition.Materialize == null)
            {
                return; // no materialized view
            }
            else if (String.IsNullOrEmpty(rdbmsQueryDefinition.Materialize.Name))
            {
                throw new InvalidOperationException($"Materialization on {materializeDefinition.Id} must have a unique name");
            }

            // Get connection and execute
            if (provider.StatementFactory.Features.HasFlag(SqlEngineFeatures.MaterializedViews))
            {

                using (var context = provider.GetWriteConnection())
                {
                    try
                    {
                        context.Open();
                        context.CommandTimeout = 360000;
                        var stmt = new SqlStatement(provider.StatementFactory.CreateSqlKeyword(SqlKeyword.RefreshMaterializedView))
                            + rdbmsQueryDefinition.Materialize.Name;

                        context.ExecuteNonQuery(stmt);
                    }
                    catch (Exception e)
                    {
                        throw new DataPersistenceException($"Error refreshing materialized view for {materializeDefinition.Id}", e);
                    }
                }
            }
        }

        /// <inheritdoc/>
        public IEnumerable<BisIndicatorMeasureResultContext> ExecuteIndicator(BiIndicatorDefinition indicatorDef, BiIndicatorPeriod period, String subjectId = null)
        {
            if (indicatorDef == null)
            {
                throw new ArgumentNullException(nameof(indicatorDef));
            }

            indicatorDef = BiUtils.ResolveRefs(indicatorDef);
            var provider = this.GetProvider(indicatorDef.Query);
            var parameters = new Dictionary<String, object>(3) {
                        { indicatorDef.Period.PeriodStartParameter, period.Start },
                        { indicatorDef.Period.PeriodEndParameter, period.End }
                    };
            if(!String.IsNullOrEmpty(subjectId))
            {
                parameters.Add(indicatorDef.Subject?.ParameterName ?? indicatorDef.Subject?.Name ?? "subject", subjectId);
            }

            // Prepare statement
            foreach (var measure in indicatorDef.Measures)
            {
                var sourceStatement = new SqlStatement("WITH source AS (").Append(this.PrepareQueryStatement(indicatorDef.Query, parameters)).Append(")");

                LinkedList<KeyValuePair<String, List<BiSqlColumnReference>>> queriesToExecute = new LinkedList<KeyValuePair<String, List<BiSqlColumnReference>>>();
                queriesToExecute.AddLast(new KeyValuePair<String, List<BiSqlColumnReference>>(String.Empty, new List<BiSqlColumnReference>() { indicatorDef.Subject }));
                foreach (var stratifier in measure.Stratifiers)
                {
                    var workingStrat = stratifier;
                    List<BiSqlColumnReference> columnsToSelectAndGroup = new List<BiSqlColumnReference>() { indicatorDef.Subject };
                    var queryName = String.Empty;
                    while (workingStrat != null)
                    {
                        queryName += $"/{workingStrat.Name}";
                        columnsToSelectAndGroup.Add(stratifier.ColumnReference);
                        queriesToExecute.AddLast(new KeyValuePair<string, List<BiSqlColumnReference>>(queryName, new List<BiSqlColumnReference>(columnsToSelectAndGroup)));
                        workingStrat = workingStrat.ThenBy;
                        
                    }
                }

                // Return the plain result set
                // Get a readonly context
                foreach (var query in queriesToExecute)
                {
                    var sqlStmt = new SqlStatement(sourceStatement).Append($"SELECT {String.Join(", ", query.Value.Select(c=>$"{c.ColumnSelector} AS {c.Name ?? c.ColumnSelector}"))}")
                        .Append($", {String.Join(", ", measure.Computation.Select(o=>this.PrepareAggregateFunction(o, o.GetColumnName())))}")
                        .Append($" FROM source GROUP BY {String.Join(", ", query.Value.Select(o=>o.ColumnSelector))} ORDER BY {String.Join(", ", query.Value.Select(o => o.ColumnSelector))}");

                    yield return new BisIndicatorMeasureResultContext(
                        indicatorDef,
                        measure,
                        query.Key,
                        parameters,
                        this,
                        new OrmQueryResultSet<ExpandoObject>(
                            new OrmResultSet<ExpandoObject>(provider.GetReadonlyConnection(), sqlStmt)
                        ),
                        DateTime.Now);
                }
            }
        }
    }
}