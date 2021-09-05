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
using SanteDB.BI;
using SanteDB.BI.Model;
using SanteDB.BI.Services;
using SanteDB.BI.Util;
using SanteDB.Core;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Exceptions;
using SanteDB.Core.Interfaces;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.OrmLite;
using SanteDB.OrmLite.Configuration;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// ADO.NET BIS Data Source
    /// </summary>
    public class OrmBiDataProvider : IBiDataSource
    {
        // Tracer
        private Tracer m_tracer = Tracer.GetTracer(typeof(OrmBiDataProvider));

        /// <summary>
        /// Executes the query
        /// </summary>
        public BisResultContext ExecuteQuery(BiQueryDefinition queryDefinition, IDictionary<string, object> parameters, BiAggregationDefinition[] aggregation, int offset, int? count)
        {
            if (queryDefinition == null)
                throw new ArgumentNullException(nameof(queryDefinition));

            // First we want to grab the connection strings used by this object
            var filledQuery = BiUtils.ResolveRefs(queryDefinition);

            // The ADO.NET provider only allows one connection to one db at a time, so verify the connections are appropriate
            if (queryDefinition.DataSources?.Count != 1)
                throw new InvalidOperationException($"ADO.NET BI queries can only source data from 1 connection source, query {queryDefinition.Name} has {queryDefinition.DataSources?.Count}");

            // Ensure we have sufficient priviledge
            var demandList = queryDefinition.DataSources.SelectMany(o => o?.MetaData.Demands);
            if (queryDefinition.MetaData?.Demands != null)
                demandList = demandList.Union(queryDefinition.MetaData?.Demands);
            foreach (var pol in demandList)
                ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(pol);

            // Apply defaults where possible
            foreach (var defaultParm in queryDefinition.Parameters.Where(p => !String.IsNullOrEmpty(p.DefaultValue) && !parameters.ContainsKey(p.Name)))
                parameters.Add(defaultParm.Name, defaultParm.DefaultValue);

            // Next we validate parameters
            if (!queryDefinition.Parameters.Where(p => p.Required == true).All(p => parameters.ContainsKey(p.Name)))
                throw new InvalidOperationException("Missing required parameter");

            // Validate parameter values
            foreach (var kv in parameters.ToArray())
            {
                var parmDef = queryDefinition.Parameters.FirstOrDefault(p => p.Name == kv.Key);
                if (parmDef == null) continue; // skip
                else switch (parmDef.Type)
                    {
                        case BiDataType.Boolean:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                                parameters[kv.Key] = DBNull.Value;
                            else
                                parameters[kv.Key] = Boolean.Parse(kv.Value.ToString());
                            break;
                        case BiDataType.Date:
                        case BiDataType.DateTime:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                                parameters[kv.Key] = DBNull.Value;
                            else
                                parameters[kv.Key] = DateTime.Parse(kv.Value.ToString());
                            break;
                        case BiDataType.Integer:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                                parameters[kv.Key] = DBNull.Value;
                            else
                                parameters[kv.Key] = Int32.Parse(kv.Value.ToString());
                            break;
                        case BiDataType.String:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                                parameters[kv.Key] = DBNull.Value;
                            else
                                parameters[kv.Key] = kv.Value.ToString();
                            break;
                        case BiDataType.Uuid:
                            if (string.IsNullOrEmpty(kv.Value?.ToString()))
                                parameters[kv.Key] = DBNull.Value;
                            else
                                parameters[kv.Key] = Guid.Parse(kv.Value.ToString());
                            break;
                        default:
                            throw new InvalidOperationException($"Cannot determine how to parse {parmDef.Type}");
                    }
            }

            // We want to open the specified connection
            var connectionString = ApplicationServiceContext.Current.GetService<IConfigurationManager>().GetConnectionString(queryDefinition.DataSources.First().ConnectionString);
            var provider =  ApplicationServiceContext.Current.GetService<IConfigurationManager>().GetSection<OrmConfigurationSection>().GetProvider(connectionString.Provider);
            provider.ConnectionString = connectionString.Value;
            provider.ReadonlyConnectionString = connectionString.Value;

            // Query definition
            var rdbmsQueryDefinition = queryDefinition.QueryDefinitions.FirstOrDefault(o => o.Invariants.Contains(provider.Invariant));
            if (rdbmsQueryDefinition == null)
                throw new InvalidOperationException($"Could not find a SQL definition for invariant {provider.Invariant} from {queryDefinition?.Id} (supported invariants: {String.Join(",", queryDefinition.QueryDefinitions.SelectMany(o=>o.Invariants))})");

            // Prepare the templated SQL
            var parmRegex = new Regex(@"\$\{([\w_][\-\d\w\._]*?)\}");
            List<Object> values = new List<object>();
            var stmt = parmRegex.Replace(rdbmsQueryDefinition.Sql, (m) =>
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
                    aggregation.FirstOrDefault(o=>o.Invariants == null);

                // Aggregation found
                if (agg == null)
                    throw new InvalidOperationException($"No provided aggregation can be found for {provider.Invariant}");

                var selector = agg.Columns?.Select(c => {
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
                        case BiAggregateFunction.Max :
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
                        $"FROM ({stmt}) {(provider.Features.HasFlag(SqlEngineFeatures.MustNameSubQuery) ? " AS _inner" : "")} " +
                    $"GROUP BY {String.Join(",", groupings)}";
            }

            // Get a readonly context
            using (var context = provider.GetReadonlyConnection())
            {
                try
                {
                    context.Open();
                    DateTime startTime = DateTime.Now;
                    var sqlStmt = new SqlStatement(provider, stmt, values.ToArray());
                    this.m_tracer.TraceInfo("Executing BI Query: {0}", context.GetQueryLiteral(sqlStmt.Build()));
                    var results = context.Query<ExpandoObject>(sqlStmt).Skip(offset).Take(count ?? 10000).ToArray();
                    return new BisResultContext(
                        queryDefinition,
                        parameters,
                        this,
                        results,
                        startTime);
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Error executing BIS data query {1} \r\n SQL: {2}\r\n Error: {0}", e, queryDefinition.Id, stmt);
                    throw new DataPersistenceException($"Error executing BIS data query", e);
                }
            }
        }

        /// <summary>
        /// Execute the specified query
        /// </summary>
        public BisResultContext ExecuteQuery(string queryId, IDictionary<string, object> parameters, BiAggregationDefinition[] aggregation, int offset, int? count)
        {
            var query = ApplicationServiceContext.Current.GetService<IBiMetadataRepository>()?.Get<BiQueryDefinition>(queryId);
            if (query == null)
                throw new KeyNotFoundException(queryId);
            else
                return this.ExecuteQuery(query, parameters, aggregation, offset, count);
        }


        /// <summary>
        /// Executes the specified view
        /// </summary>
        public BisResultContext ExecuteView(BiViewDefinition viewDef, IDictionary<string, object> parameters, int offset, int? count)
        {
            viewDef = BiUtils.ResolveRefs(viewDef) as BiViewDefinition;
            var retVal = this.ExecuteQuery(viewDef.Query, parameters, viewDef.AggregationDefinitions?.ToArray(), offset, count);
            if(viewDef.Pivot != null)
                retVal = ApplicationServiceContext.Current.GetService<IBiPivotProvider>().Pivot(retVal, viewDef.Pivot);
            return retVal;
        }


    }
}
