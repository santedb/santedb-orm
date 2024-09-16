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
using SanteDB.BI;
using SanteDB.BI.Model;
using SanteDB.BI.Services;
using SanteDB.BI.Services.Impl;
using SanteDB.BI.Util;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Exceptions;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Configuration;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
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
        /// Executes the query
        /// </summary>
        public BisResultContext ExecuteQuery(BiQueryDefinition queryDefinition, IDictionary<string, object> parameters, BiAggregationDefinition[] aggregation, int? offset = null, int? count = null)
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

                var selector = agg.Columns?.Select(c =>
                {
                    switch (c.Aggregation)
                    {
                        case BiAggregateFunction.Average:
                            return $"AVG({c.ColumnSelector}) AS {c.Name}";

                        case BiAggregateFunction.Count:
                            return $"COUNT({c.ColumnSelector}) AS {c.Name}";

                        case BiAggregateFunction.CountDistinct:
                            return $"COUNT(DISTINCT {c.ColumnSelector}) AS {c.Name}";

                        case BiAggregateFunction.First:
                            return $"FIRST({c.ColumnSelector}) AS {c.Name}";

                        case BiAggregateFunction.Last:
                            return $"LAST({c.ColumnSelector}) AS {c.Name}";

                        case BiAggregateFunction.Max:
                            return $"MAX({c.ColumnSelector}) AS {c.Name}";

                        case BiAggregateFunction.Min:
                            return $"MIN({c.ColumnSelector}) AS {c.Name}";

                        case BiAggregateFunction.Sum:
                            return $"SUM({c.ColumnSelector}) AS {c.Name}";

                        case BiAggregateFunction.Value:
                            return $"{c.ColumnSelector} AS {c.Name}";

                        default:
                            throw new InvalidOperationException("Cannot apply aggregation function");
                    }
                }).ToArray() ?? new string[] { "*" };
                String[] groupings = agg.Groupings.Select(g => g.ColumnSelector).ToArray(),
                    colGroupings = agg.Groupings.Select(g => $"{g.ColumnSelector} AS {g.Name}").ToArray();
                // Aggregate
                stmt = $"SELECT {String.Join(",", colGroupings.Concat(selector))} " +
                        $" FROM ({stmt})  AS _inner ";


                stmt += $" GROUP BY {String.Join(",", groupings)}";
                if (agg.Sorting != null)
                {
                    stmt += $" ORDER BY {String.Join(",", agg.Sorting.Select(o => o.Name ?? o.ColumnSelector))}";
                }
            }

            // Get a readonly context
            try
            {
                DateTime startTime = DateTime.Now;
                var sqlStmt = new SqlStatement(stmt, values.ToArray());
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
                this.m_tracer.TraceError("Error executing BIS data query {1} \r\n SQL: {2}\r\n Error: {0}", e, queryDefinition.Id, stmt);
                throw new DataPersistenceException($"Error executing BIS data query", e);
            }
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

    }
}