using SanteDB.Core.Model;
using SanteDB.Core.Services;
using SanteDB.OrmLite;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SanteDB.OrmLite.MappedResultSets
{
    /// <summary>
    /// Represents an ADO query provider which is used by wrapped result set
    /// instance to calculate its result set.
    /// </summary>
    public interface IMappedQueryProvider<TModel>
    {
        /// <summary>
        /// Get the provider
        /// </summary>
        IDbProvider Provider { get; }

        /// <summary>
        /// Gets the instance of the query persistence service
        /// </summary>
        IQueryPersistenceService QueryPersistence { get; }

        /// <summary>
        /// Execute a query for the specified model with query identifier returning the results
        /// </summary>
        /// <param name="query">The query to execute</param>
        /// <returns>The amount of results to return</returns>
        IOrmResultSet ExecuteQueryOrm(DataContext context, Expression<Func<TModel, bool>> query);

        /// <summary>
        /// Get the specified object at the specified key
        /// </summary>
        TModel Get(DataContext context, Guid key);

        /// <summary>
        /// Convert <paramref name="result"/> to model
        /// </summary>
        TModel ToModelInstance(DataContext context, object result);

        /// <summary>
        /// Map sort expression
        /// </summary>
        /// <param name="sortExpression">The sort expression to map</param>
        /// <returns>The mapped sort expression</returns>
        Expression MapSortExpression(Expression<Func<TModel, dynamic>> sortExpression);
    }
}