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
 * User: fyfej
 * Date: 2023-6-21
 */
using SanteDB;
using SanteDB.BI.Datamart;
using SanteDB.BI.Datamart.DataFlow;
using SanteDB.BI.Model;
using SanteDB.Core;
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.i18n;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.OrmLite.Attributes;
using SanteDB.OrmLite.Configuration;
using SanteDB.OrmLite.Migration;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// A data integrator which uses the ORM classes 
    /// </summary>
    public class OrmBiDataIntegrator : IDataIntegrator
    {

        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(OrmBiDataIntegrator));
        private readonly IDataFlowExecutionContext m_executionContext;
        private readonly IDbProvider m_provider;
        private readonly IDataConfigurationProvider m_configurationProvider;
        private readonly ConnectionString m_connectionString;
        private DataContext m_currentContext;
        private bool m_disposed = false;
        private readonly Regex m_sqlSafeName = new Regex(@"^[\w_][\w_0-9]+$", RegexOptions.Compiled);
        private readonly IDbStatementFactory m_statementFactory;
        private readonly IPolicyEnforcementService m_pepService;


        /// <summary>
        /// Metadata system table
        /// </summary>
        [Table("meta_systbl")]
        private class OrmBiDatamartMetadata
        {

            /// <summary>
            /// Gets or sets the schema object name
            /// </summary>
            [Column("obj_name"), PrimaryKey, NotNull]
            public String SchemaObjectName { get; set; }

            /// <summary>
            /// Gets or sets the last migration date
            /// </summary>
            [Column("mig_utc"), NotNull]
            public DateTimeOffset LastMigration { get; set; }

            /// <summary>
            /// Gets or sets the schema object hash
            /// </summary>
            [Column("hash"), NotNull]
            public byte[] SchemaObjectHash { get; set; }

        }

        /// <summary>
        /// Metadata system table
        /// </summary>
        [Table("meta_dep_systbl")]
        private class OrmBiDatamartDependencyMetadata
        {

            /// <summary>
            /// Gets or sets the schema object name
            /// </summary>
            [Column("obj_name"), PrimaryKey, NotNull, ForeignKey(typeof(OrmBiDatamartMetadata), nameof(OrmBiDatamartMetadata.SchemaObjectName))]
            public String SchemaObjectName { get; set; }

            /// <summary>
            /// Gets or sets the last migration date
            /// </summary>
            [Column("dep_obj_name"), PrimaryKey, NotNull, ForeignKey(typeof(OrmBiDatamartMetadata), nameof(OrmBiDatamartMetadata.SchemaObjectName))]
            public String DependsOnObjectName { get; set; }

        }

        /// <summary>
        /// Creates a new data integrator
        /// </summary>
        public OrmBiDataIntegrator(IDataFlowExecutionContext executionContext, ConnectionString connectionString, BiDataSourceDefinition dataSourceDefinition)
        {

            this.m_executionContext = executionContext;
            var ormConfiguration = ApplicationServiceContext.Current.GetService<IConfigurationManager>().GetSection<OrmConfigurationSection>();
            this.m_provider = ormConfiguration.GetProvider(connectionString.Provider);
            if (this.m_provider == null)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.TYPE_NOT_FOUND, connectionString.Provider));
            }
            this.m_provider.ConnectionString = connectionString.Value;
            this.m_configurationProvider = this.m_provider.GetDataConfigurationProvider();
            this.m_connectionString = connectionString;
            this.m_statementFactory = this.m_provider.StatementFactory;
            this.m_pepService = ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>();

            this.DataSource = new BiDataSourceDefinition()
            {
                ConnectionString = dataSourceDefinition.ConnectionString,
                Id = dataSourceDefinition.Id,
                Label = dataSourceDefinition.Label,
                Identifier = dataSourceDefinition.Identifier,
                Status = BiDefinitionStatus.Active,
                MetaData = new BiMetadata()
                {
                    IsPublic = false,
                    Demands = dataSourceDefinition.MetaData?.Demands
                },
                ProviderType = typeof(OrmBiDataProvider),
                Name = dataSourceDefinition.Name
            };
        }


        /// <summary>
        /// Get the data source object represented by this object
        /// </summary>
        public BiDataSourceDefinition DataSource { get; }

        /// <summary>
        /// Create a migration registration record
        /// </summary>
        /// <returns></returns>
        private OrmBiDatamartMetadata CreateMetadataRegistration(BiSchemaObjectDefinition schemaObjectDefinition)
        {
            this.ThrowIfInvalid(schemaObjectDefinition);
            using (var ms = new MemoryStream())
            {
                schemaObjectDefinition.Save(ms);
                return new OrmBiDatamartMetadata()
                {
                    LastMigration = DateTimeOffset.Now,
                    SchemaObjectName = schemaObjectDefinition.Name,
                    SchemaObjectHash = SHA256.Create().ComputeHash(ms.ToArray())
                };
            }
        }

        /// <summary>
        /// Throw if the execution of this method is not allowed for this purpose
        /// </summary>
        private void ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType expectedPurpose)
        {
            if (!this.m_executionContext.Purpose.HasFlag(expectedPurpose))
            {
                throw new NotSupportedException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, expectedPurpose));
            }
        }

        /// <summary>
        /// Throw if the execution of this method is not allowed for this purpose
        /// </summary>
        private void ThrowIfHasPurpose(DataFlowExecutionPurposeType expectedPurpose)
        {
            if (this.m_executionContext.Purpose.HasFlag(expectedPurpose))
            {
                throw new NotSupportedException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, expectedPurpose));
            }
        }

        /// <summary>
        /// Make an object name safe for SQL
        /// </summary>
        private void ThrowIfInvalid(BiSchemaObjectDefinition schemaObjectDefinition)
        {
            if (String.IsNullOrEmpty(schemaObjectDefinition.Name))
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.MISSING_VALUE, nameof(BiSchemaObjectDefinition.Name)));
            }
            else if (!this.m_sqlSafeName.IsMatch(schemaObjectDefinition.Name))
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_FORMAT, schemaObjectDefinition.Name, this.m_sqlSafeName));
            }
        }

        /// <summary>
        /// Throw an exception if this object is disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (this.m_disposed)
            {
                throw new ObjectDisposedException(nameof(OrmBiDataIntegrator));
            }
        }

        /// <summary>
        /// Throw if the connection is readonly
        /// </summary>
        private void ThrowIfReadonly()
        {
            if (this.m_currentContext?.IsReadonly == true)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.CANT_WRITE_READ_ONLY_STREAM));
            }
        }

        /// <summary>
        /// Throw if the connection is not open
        /// </summary>
        private void ThrowIfNotOpen()
        {
            if (this.m_currentContext?.Connection.State != System.Data.ConnectionState.Open)
            {
                throw new InvalidOperationException(ErrorMessages.NOT_INITIALIZED);
            }
        }

        /// <summary>
        /// Throw if the connection is open
        /// </summary>
        private void ThrowIfOpen(string methodName)
        {
            if (this.m_currentContext?.Connection.State == System.Data.ConnectionState.Open)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, methodName));
            }
        }

        /// <inheritdoc/>
        public IDisposable BeginTransaction()
        {
            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfReadonly();
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.Refresh);
            if (this.m_currentContext.Transaction != null)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(BeginTransaction)));
            }
            return this.m_currentContext.BeginTransaction();
        }

        /// <inheritdoc/>
        public void CommitTransaction()
        {
            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.Refresh);

            if (this.m_currentContext.Transaction == null)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(CommitTransaction)));
            }
            this.m_currentContext.Transaction.Commit();
        }

        /// <inheritdoc/>
        public void RecreateObject(BiSchemaObjectDefinition objectToCreate)
        {
            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfReadonly();
            this.ThrowIfInvalid(objectToCreate);

            if (!this.NeedsMigration(objectToCreate))
            {
                if (objectToCreate is BiSchemaViewDefinition vd && vd.IsMaterialized)
                {
                    this.m_tracer.TraceInfo("Refreshing {0}...", vd.Name);
                    this.m_currentContext.ExecuteNonQuery($"{this.m_statementFactory.CreateSqlKeyword(SqlKeyword.RefreshMaterializedView)} {vd.Name}");
                }
                return;
            }

            // Demands?
            objectToCreate.MetaData?.Demands?.ForEach(o => this.m_pepService.Demand(o));

            var objectExists = this.Exists(objectToCreate);
            var statementQueue = new ConcurrentQueue<SqlStatement>();
            var dependencies = new List<OrmBiDatamartDependencyMetadata>();
            string[] deletedObjects = null, createdSupportingObjects = null;

            switch (objectToCreate)
            {
                case BiSchemaTableDefinition table:
                    {
                        if (!table.Temporary)
                        {
                            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.SchemaManagement);
                        }
                        else
                        {
                            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.Refresh);
                        }

                        if (objectExists)
                        {
                            var dropStack = this.GetDependentObjects(table);
                            dropStack.Distinct().ToList().ForEach(o => statementQueue.Enqueue(new SqlStatement($"DROP {(o.Contains("VW") ? "VIEW" : "TABLE")} {o}")));
                            deletedObjects = dropStack.Distinct().ToArray();
                        }

                        // Create the table 
                        var statementBuilder = this.m_currentContext.CreateSqlStatementBuilder();
                        var constraintList = new LinkedList<SqlStatement>();

                        statementBuilder.Append($"CREATE {(table.Temporary ? "TEMPORARY" : String.Empty)} TABLE {table.Name} (");

                        foreach (var col in table.Columns)
                        {
                            statementBuilder.Append($"{col.Name} {this.GetDataType(col)}");
                            if (col.NotNull)
                            {
                                statementBuilder.Append(" NOT NULL ");
                            }
                            if (col.IsUnique)
                            {
                                statementBuilder.Append(" UNIQUE ");
                            }
                            statementBuilder.Append(",");

                            if (col.References?.Resolved is BiSchemaTableDefinition otherTable)
                            {
                                var pkOther = this.GetPrimaryKey(otherTable);
                                constraintList.AddLast(new SqlStatement($"CONSTRAINT FK_{table.Name}_{col.Name} FOREIGN KEY ({col.Name}) REFERENCES {otherTable.Name}({pkOther.Name}) {statementBuilder.DbProvider.Provider.StatementFactory.CreateSqlKeyword(SqlKeyword.DeferConstraints)}"));
                                dependencies.Add(new OrmBiDatamartDependencyMetadata()
                                {
                                    SchemaObjectName = table.Name,
                                    DependsOnObjectName = otherTable.Name
                                });
                            }

                            if (col.IsKey)
                            {
                                constraintList.AddLast(new SqlStatement($"CONSTRAINT PK_{table.Name} PRIMARY KEY ({col.Name})"));
                            }
                        }

                        if (table.Parent != null)
                        {
                            if (!(table.Parent.Resolved is BiSchemaTableDefinition parentTable))
                            {
                                this.m_tracer.TraceError("The reference to {0} has not been resolved - should call ResolveRefs() before this function", table.Parent.Ref);
                                throw new InvalidOperationException(ErrorMessages.NOT_INITIALIZED);
                            }
                            var parentKey = this.GetPrimaryKey(parentTable);
                            statementBuilder.Append($"{parentKey.Name} {this.GetDataType(parentKey)} NOT NULL").Append(",");
                            constraintList.AddLast(new SqlStatement($"CONSTRAINT FK_{table.Name}_{parentTable.Name} FOREIGN KEY ({parentKey.Name}) REFERENCES {parentTable.Name}({parentKey.Name})"));
                            if (!table.Columns.Any(o => o.IsKey))
                            {
                                constraintList.AddLast(new SqlStatement($"CONSTRAINT PK_{table.Name} PRIMARY KEY ({parentKey.Name})"));
                            }

                            dependencies.Add(new OrmBiDatamartDependencyMetadata()
                            {
                                SchemaObjectName = table.Name,
                                DependsOnObjectName = parentTable.Name
                            });

                        }

                        foreach (var itm in constraintList)
                        {
                            statementBuilder.Append(itm).Append(",");
                        }

                        statementQueue.Enqueue(statementBuilder.RemoveLast(out _).Append(")").Statement.Prepare());

                        // Indexes
                        foreach (var col in table.Columns.Where(o => o.IsIndex))
                        {
                            statementQueue.Enqueue(new SqlStatement($"CREATE INDEX {table.Name}_{col.Name}_IDX ON {table.Name}({col.Name})"));
                        }

                        // Create a parent view
                        if (table.Parent?.Resolved is BiSchemaTableDefinition joinTable)
                        {
                            var joinKey = this.GetPrimaryKey(joinTable);
                            statementBuilder = this.m_currentContext.CreateSqlStatementBuilder($"CREATE VIEW VW_{table.Name} AS ");
                            statementBuilder.Append($"SELECT {String.Join(",", table.Columns.Select(o => $"{table.Name}.{o.Name}"))}").Append(",");
                            var fromClause = this.m_currentContext.CreateSqlStatementBuilder($" FROM {table.Name} ");
                            while (joinTable != null)
                            {

                                fromClause.Append($" INNER JOIN {joinTable.Name} USING ({joinKey.Name}) ");
                                statementBuilder.Append(String.Join(",", joinTable.Columns.Select(o => $"{joinTable.Name}.{o.Name}"))).Append(",");
                                joinTable = joinTable.Parent?.Resolved as BiSchemaTableDefinition;
                            }

                            statementQueue.Enqueue(statementBuilder.RemoveLast(out _).Append(fromClause).Statement.Prepare());
                            createdSupportingObjects = new string[] { $"VW_{table.Name}" };
                        }

                        break;
                    }
                case BiSchemaViewDefinition view:
                    {
                        this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.SchemaManagement);

                        // Do we have a definition
                        var sqlDefinition = view.Query.FirstOrDefault(o => o.Invariants.Contains(this.m_provider.Invariant));
                        if (sqlDefinition == null)
                        {
                            throw new InvalidOperationException(String.Format(ErrorMessages.DIALECT_NOT_FOUND, this.m_provider.Invariant));
                        }

                        if (objectExists)
                        {
                            statementQueue.Enqueue(new SqlStatement($"DROP VIEW {view.Name}"));
                        }

                        var statementBuilder = this.m_currentContext.CreateSqlStatementBuilder();

                        if (view.IsMaterialized)
                        {
                            statementBuilder.Append(this.m_statementFactory.CreateSqlKeyword(SqlKeyword.CreateMaterializedView));
                        }
                        else
                        {
                            statementBuilder.Append(this.m_statementFactory.CreateSqlKeyword(SqlKeyword.CreateView));
                        }

                        statementBuilder.Append($" {view.Name} AS {sqlDefinition.Sql}");
                        statementQueue.Enqueue(statementBuilder.Statement.Prepare());

                        break;
                    }
                default:
                    throw new ArgumentOutOfRangeException(nameof(objectToCreate));
            }

            // Insert the SQL statements
            while (statementQueue.TryDequeue(out var statement))
            {
                this.m_tracer.TraceVerbose("BI EXEC: {0}", statement.ToLiteral());
                this.m_currentContext.ExecuteNonQuery(statement);
            }

            if ((objectToCreate as BiSchemaTableDefinition)?.Temporary != true)
            {
                // Now record the migration
                var migration = this.CreateMetadataRegistration(objectToCreate);

                if (deletedObjects?.Any() == true)
                {
                    this.m_currentContext.DeleteAll<OrmBiDatamartDependencyMetadata>(o => deletedObjects.Contains(o.SchemaObjectName) || deletedObjects.Contains(o.DependsOnObjectName));
                    this.m_currentContext.DeleteAll<OrmBiDatamartMetadata>(o => deletedObjects.Contains(o.SchemaObjectName));
                }

                this.m_currentContext.DeleteAll<OrmBiDatamartDependencyMetadata>(o => o.SchemaObjectName == migration.SchemaObjectName || o.DependsOnObjectName == migration.SchemaObjectName);
                this.m_currentContext.Delete(migration);
                this.m_currentContext.Insert(migration);
                this.m_currentContext.InsertOrUpdateAll(dependencies);
                if (createdSupportingObjects?.Any() == true)
                {
                    this.m_currentContext.InsertAll(createdSupportingObjects.Select(o => new OrmBiDatamartMetadata() { LastMigration = DateTimeOffset.Now, SchemaObjectName = o, SchemaObjectHash = new byte[0] }));
                    this.m_currentContext.InsertAll(createdSupportingObjects.Select(o => new OrmBiDatamartDependencyMetadata() { SchemaObjectName = o, DependsOnObjectName = migration.SchemaObjectName }));
                }
            }
        }

        /// <summary>
        /// Get all dependent objects
        /// </summary>
        private IEnumerable<String> GetDependentObjects(BiSchemaTableDefinition table)
        {
            // Get all objects that depend on this
            var dropStack = new List<String>() { table.Name };
            dropStack.AddRange(this.m_currentContext.Query<OrmBiDatamartDependencyMetadata>(o => o.DependsOnObjectName == table.Name).Select(o => o.SchemaObjectName));
            for (var i = 0; i < dropStack.Count; i++)
            {
                var itm = dropStack[i];
                ExtensionMethods.ForEach(this.m_currentContext.Query<OrmBiDatamartDependencyMetadata>(o => o.DependsOnObjectName == itm)
                        .Select(o => o.SchemaObjectName)
                        .AsEnumerable(), o =>
                        {
                            if (!dropStack.Contains(o))
                            {
                                dropStack.Add(o);
                            }
                        });
            }
            dropStack.Reverse();
            return dropStack;
        }

        /// <summary>
        /// Map data type 
        /// </summary>
        private String GetDataType(BiSchemaColumnDefinition columnDefinition)
        {
            var schemaType = this.GetDataType(columnDefinition.Type);
            if (schemaType == typeof(Object))
            {
                if (!(columnDefinition.References.Resolved is BiSchemaTableDefinition otherTable))
                {
                    this.m_tracer.TraceError("The reference to {0} has not been resolved - should call ResolveRefs() before this function", columnDefinition.References.Ref);
                    throw new InvalidOperationException(ErrorMessages.NOT_INITIALIZED);
                }
                var pkOther = this.GetPrimaryKey(otherTable);
                return this.GetDataType(pkOther);
            }
            else
            {
                return this.m_provider.MapSchemaDataType(schemaType);
            }
        }

        /// <summary>
        /// Get .NET datatype
        /// </summary>
        private Type GetDataType(BiDataType type)
        {
            switch (type)
            {
                case BiDataType.Boolean:
                    return typeof(Boolean);
                case BiDataType.Date:
                    return typeof(DateTime);
                case BiDataType.DateTime:
                    return typeof(DateTimeOffset);
                case BiDataType.Integer:
                    return typeof(Int64);
                case BiDataType.String:
                    return typeof(String);
                case BiDataType.Uuid:
                    return typeof(Guid);
                case BiDataType.Decimal:
                    return typeof(Decimal);
                case BiDataType.Ref:
                    return typeof(Object);
                case BiDataType.Float:
                    return typeof(double);
                case BiDataType.Binary:
                    return typeof(byte[]);
                default:
                    throw new ArgumentOutOfRangeException(nameof(type));
            }
        }

        /// <summary>
        /// Get primary key
        /// </summary>
        private BiSchemaColumnDefinition GetPrimaryKey(BiSchemaTableDefinition table)
        {
            var retVal = table.Columns.FirstOrDefault(o => o.IsKey);
            if (retVal != null)
            {
                return retVal;
            }
            else if (table.Parent != null && table.Parent.Resolved is BiSchemaTableDefinition parentTable)
            {
                return this.GetPrimaryKey(parentTable);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.NO_DATA_KEY_DEFINED, table.Name));
            }
        }

        /// <inheritdoc/>
        public void CreateDatabase()
        {
            this.ThrowIfDisposed();
            this.ThrowIfOpen(nameof(CreateDatabase));
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.DatabaseManagement);

            var databaseName = this.m_connectionString.GetComponent(this.m_configurationProvider.Capabilities.NameSetting);
            if (this.DatabaseExists())
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.DUPLICATE_OBJECT, databaseName));
            }

            // User must be able to administer warehouse
            this.m_pepService.Demand(PermissionPolicyIdentifiers.AdministerWarehouse);

            this.m_configurationProvider.CreateDatabase(this.m_connectionString, this.m_connectionString.GetComponent(this.m_configurationProvider.Capabilities.NameSetting), String.Empty);
            using (var context = this.m_provider.GetWriteConnection())
            {
                context.Open();
                context.CreateTable<OrmBiDatamartMetadata>();
                context.CreateTable<OrmBiDatamartDependencyMetadata>();
                context.Close();
            }
        }

        /// <inheritdoc/>
        public dynamic Delete(BiSchemaTableDefinition target, dynamic dataToDelete)
        {
            if (target == null)
            {
                throw new ArgumentNullException(nameof(target));
            }
            else if (dataToDelete == null)
            {
                throw new ArgumentNullException(nameof(dataToDelete));
            }

            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfReadonly();
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.Refresh);
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.ThrowIfDisposed();
            this.m_currentContext?.Dispose();
            this.m_currentContext = null;
            this.m_disposed = true;
        }

        /// <inheritdoc/>
        public void DropObject(BiSchemaObjectDefinition objectToDrop)
        {
            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfReadonly();
            this.ThrowIfInvalid(objectToDrop);
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.SchemaManagement);

            // Demands?
            objectToDrop.MetaData?.Demands?.ForEach(o => this.m_pepService.Demand(o));


        }

        /// <inheritdoc/>
        public void DropDatabase()
        {
            this.ThrowIfDisposed();
            this.ThrowIfOpen(nameof(DropDatabase));
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.DatabaseManagement);

            // User must be able to administer warehouse
            this.m_pepService.Demand(PermissionPolicyIdentifiers.AdministerWarehouse);

            var databaseName = this.m_connectionString.GetComponent(this.m_configurationProvider.Capabilities.NameSetting);
            if (!this.DatabaseExists())
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.OBJECT_NOT_FOUND, databaseName));
            }

            this.m_configurationProvider.DropDatabase(this.m_connectionString, databaseName);

        }

        /// <inheritdoc/>
        public void ExecuteNonQuery(BiSqlDefinition sql)
        {
            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
        }

        /// <inheritdoc/>
        public dynamic Insert(BiSchemaTableDefinition target, dynamic dataToInsert)
        {
            if (target == null)
            {
                throw new ArgumentNullException(nameof(target));
            }
            else if (dataToInsert == null)
            {
                throw new ArgumentNullException(nameof(dataToInsert));
            }

            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfReadonly();
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.Refresh);

            // Demands?
            target.MetaData?.Demands?.ForEach(o => this.m_pepService.Demand(o));

            if (!(dataToInsert is IDictionary<String, Object> dictInsert))
            {
                throw new ArgumentException(nameof(dataToInsert));
            }

            // Prepare an insert statement
            var colNames = target.Columns.Select(o => o.Name).ToList();
            var colTypes = target.Columns.Select(o => o.Type).ToList();
            if (target.Parent != null)
            {
                var pk = this.GetPrimaryKey(target);
                colNames.Add(pk.Name);
                colTypes.Add(pk.Type);
            }

            var values = Enumerable.Range(0, colNames.Count)
                .Select(o =>
                {
                    if (dictInsert.TryGetValue(colNames[o], out var v) || dictInsert.TryGetValue(colNames[o].ToLowerInvariant(), out v) || dictInsert.TryGetValue(colNames[o].ToUpperInvariant(), out v))
                    {
                        return this.m_provider.ConvertValue(v, this.GetDataType(colTypes[o]));
                    }
                    return DBNull.Value;
                }).ToArray();



            var stmt = this.m_currentContext.CreateSqlStatementBuilder($"INSERT INTO {target.Name} (")
                .Append(String.Join(",", colNames))
                .Append(") VALUES (")
                .Append(String.Join(",", colNames.Select(o => "?")), values)
                .Append(") RETURNING ")
                .Append(String.Join(",", colNames));

            return this.m_currentContext.FirstOrDefault<ExpandoObject>(stmt.Statement.Prepare());
        }

        /// <inheritdoc/>
        public dynamic InsertOrUpdate(BiSchemaTableDefinition target, dynamic dataToInsert)
        {
            if (target == null)
            {
                throw new ArgumentNullException(nameof(target));
            }
            else if (dataToInsert == null)
            {
                throw new ArgumentNullException(nameof(dataToInsert));
            }

            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfReadonly();
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.Refresh);

            // Demands?
            target.MetaData?.Demands?.ForEach(o => this.m_pepService.Demand(o));

            // Check if the PK exists - if it does update if not insert
            if (!(dataToInsert is IDictionary<String, Object> dictInsert))
            {
                throw new ArgumentException(nameof(dataToInsert));
            }

            var pkCol = this.GetPrimaryKey(target);
            if (!dictInsert.TryGetValue(pkCol.Name, out var pkValue) && !dictInsert.TryGetValue(pkCol.Name.ToLowerInvariant(), out pkValue))
            {
                return this.Insert(target, dataToInsert);
            }

            var checkStmt = new SqlStatement($"SELECT 1 FROM {target.Name} WHERE {pkCol.Name} = ?", pkValue);
            if (this.m_currentContext.Any(checkStmt))
            {
                return this.Update(target, dataToInsert);
            }
            else
            {
                return this.Insert(target, dataToInsert);
            }
        }

        /// <inheritdoc/>
        public void OpenRead()
        {
            this.ThrowIfDisposed();
            this.ThrowIfOpen(nameof(OpenRead));
            this.m_pepService.Demand(PermissionPolicyIdentifiers.QueryWarehouseData);
            this.m_currentContext = this.m_provider.GetReadonlyConnection();
            this.m_currentContext.Open();
        }


        /// <inheritdoc/>
        public void OpenWrite()
        {
            this.ThrowIfDisposed();
            this.ThrowIfOpen(nameof(OpenWrite));
            this.ThrowIfHasPurpose(DataFlowExecutionPurposeType.Discovery);
            this.m_pepService.Demand(PermissionPolicyIdentifiers.WriteWarehouseData);
            this.m_currentContext = this.m_provider.GetWriteConnection();
            this.m_currentContext.Open();
        }


        /// <inheritdoc/>
        public IEnumerable<dynamic> Query(IEnumerable<BiSqlDefinition> queryToExecute, BiSchemaTableDefinition expectedOutput = null)
        {
            if (queryToExecute == null)
            {
                throw new ArgumentNullException(nameof(queryToExecute));
            }

            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();

            // Find the SQL definition which matches this definition
            var sqlDef = queryToExecute.FirstOrDefault(o => o.Invariants.Contains(this.m_provider.Invariant));
            if (sqlDef == null)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.DIALECT_NOT_FOUND, this.m_provider.Invariant));
            }

            // Execute the SQL
            foreach (IDictionary<String, Object> tuple in new OrmResultSet<ExpandoObject>(this.m_currentContext, new SqlStatement(sqlDef.Sql)))
            {
                if (expectedOutput != null)
                {
                    var pkColumn = this.GetPrimaryKey(expectedOutput);
                    var targetSchemaList = expectedOutput.Columns.ToDictionary(o => o.Name.ToLowerInvariant(), o => o.Type);
                    if (!targetSchemaList.ContainsKey(pkColumn.Name.ToLowerInvariant())) // add the linking column
                    {
                        targetSchemaList.Add(pkColumn.Name.ToLowerInvariant(), pkColumn.Type);
                    }
                    yield return targetSchemaList.ToDictionary(o => o.Key, o => this.m_provider.ConvertValue(tuple[o.Key], this.GetDataType(o.Value)));
                }
                else
                {
                    yield return tuple;
                }
            }
        }

        /// <inheritdoc/>
        public void RollbackTransaction()
        {
            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.Refresh);

            if (this.m_currentContext.Transaction == null)
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.WOULD_RESULT_INVALID_STATE, nameof(CommitTransaction)));
            }
            this.m_currentContext.Transaction.Rollback();
        }

        /// <inheritdoc/>
        public void TruncateObject(BiSchemaObjectDefinition objectToTruncate)
        {

            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfReadonly();
            this.ThrowIfInvalid(objectToTruncate);
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.Refresh);

            // Demands?
            objectToTruncate.MetaData?.Demands?.ForEach(o => this.m_pepService.Demand(o));

            // Truncate all dependent objects
            if (objectToTruncate is BiSchemaTableDefinition table)
            {
                foreach (var trunc in this.GetDependentObjects(table).Where(o => !o.Contains("VW")))
                {
                    if (this.m_provider.StatementFactory.Features.HasFlag(SqlEngineFeatures.Truncate))
                    {
                        this.m_currentContext.ExecuteNonQuery($"TRUNCATE TABLE {trunc} {(this.m_provider.StatementFactory.Features.HasFlag(SqlEngineFeatures.Cascades) ? "CASCADE" : String.Empty)}");
                    }
                    else
                    {
                        this.m_currentContext.ExecuteNonQuery($"DELETE FROM {trunc} {(this.m_provider.StatementFactory.Features.HasFlag(SqlEngineFeatures.Cascades) ? "CASCADE" : String.Empty)}");
                    }
                }
            }
        }

        /// <inheritdoc/>
        public dynamic Update(BiSchemaTableDefinition target, dynamic dataToUpdate)
        {
            if (target == null)
            {
                throw new ArgumentNullException(nameof(target));
            }
            else if (dataToUpdate == null)
            {
                throw new ArgumentNullException(nameof(dataToUpdate));
            }

            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfReadonly();
            this.ThrowIfDoesNotHavePurpose(DataFlowExecutionPurposeType.Refresh);

            // Update
            target.MetaData?.Demands?.ForEach(o => this.m_pepService.Demand(o));

            // Extract PK col
            if (!(dataToUpdate is IDictionary<String, Object> dictInsert))
            {
                throw new ArgumentException(nameof(dataToUpdate));
            }

            // No primary key so we insert
            var pkCol = this.GetPrimaryKey(target);
            if (!dictInsert.TryGetValue(pkCol.Name, out var pkValue) && !dictInsert.TryGetValue(pkCol.Name.ToLowerInvariant(), out pkValue))
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.DATA_STRUCTURE_NOT_APPROPRIATE, target.Name, "No Primary Key Value"));
            }

            var values = target.Columns
                .Select(o =>
                {
                    if (dictInsert.TryGetValue(o.Name, out var v) || dictInsert.TryGetValue(o.Name.ToLowerInvariant(), out v) || dictInsert.TryGetValue(o.Name.ToUpperInvariant(), out v))
                    {
                        return new KeyValuePair<String, Object>(o.Name, this.m_provider.ConvertValue(v, this.GetDataType(o.Type)));
                    }
                    return new KeyValuePair<String, Object>(o.Name, DBNull.Value);
                });


            var stmt = this.m_currentContext.CreateSqlStatementBuilder($"UPDATE {target.Name} SET ");
            values.ToList().ForEach(v =>
            {
                stmt.Append($" {v.Key} = ? ", v.Value).Append(",");
            });
            stmt.RemoveLast(out _).Append($" WHERE {pkCol.Name} = ?", pkValue)
                .Append(" RETURNING ")
                .Append(String.Join(",", target.Columns.Select(o => o.Name))).Append(",").Append(pkCol.Name);

            return this.m_currentContext.FirstOrDefault<ExpandoObject>(stmt.Statement.Prepare());

        }

        /// <inheritdoc/>
        public bool DatabaseExists() => this.m_configurationProvider.GetDatabases(this.m_connectionString).Any(db => db == this.m_connectionString.GetComponent(this.m_configurationProvider.Capabilities.NameSetting));

        /// <inheritdoc/>
        public void Close()
        {
            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.m_currentContext.Close();
        }

        /// <inheritdoc/>
        public bool Exists(BiSchemaObjectDefinition objectToCheck)
        {
            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfInvalid(objectToCheck);
            return this.m_currentContext.Query<OrmBiDatamartMetadata>(o => o.SchemaObjectName == objectToCheck.Name).Any();
        }

        /// <inheritdoc/>
        public bool NeedsMigration(BiSchemaObjectDefinition objectToCheck)
        {
            this.ThrowIfDisposed();
            this.ThrowIfNotOpen();
            this.ThrowIfInvalid(objectToCheck);
            var expectedMigrationRecord = this.CreateMetadataRegistration(objectToCheck);
            return !this.m_currentContext.Query<OrmBiDatamartMetadata>(o => o.SchemaObjectName == expectedMigrationRecord.SchemaObjectName && o.SchemaObjectHash == expectedMigrationRecord.SchemaObjectHash).Any();

        }

    }
}
