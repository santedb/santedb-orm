using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Represents a result set from enumerable data
    /// </summary>
    /// <typeparam name="TData">The type of record this result set holds</typeparam>
    public class OrmResultSet<TData> : IEnumerable<TData> , IOrmResultSet
    {

        /// <summary>
        /// Gets the SQL statement that this result set is based on
        /// </summary>
        public SqlStatement Statement { get; }

        /// <summary>
        /// Get the context
        /// </summary>
        public DataContext Context { get; }

        /// <summary>
        /// Create a new result set based on the context and statement
        /// </summary>
        /// <param name="stmt">The SQL Statement</param>
        /// <param name="context">The data context on which data should be executed</param>
        internal OrmResultSet(DataContext context, SqlStatement stmt)
        {
            this.Statement = stmt;
            this.Context = context;
        }

        /// <summary>
        /// Instructs the reader to skip n records
        /// </summary>
        public OrmResultSet<TData> Skip(int n)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().Offset(n));
        }

        /// <summary>
        /// Instructs the reader to take <paramref name="n"/> records
        /// </summary>
        /// <param name="n">The number of records to take</param>
        public OrmResultSet<TData> Take(int n)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().Limit(n));
        }

        /// <summary>
        /// Instructs the reader to count the number of records
        /// </summary>
        public int Count()
        {
            return this.Context.Count(this.Statement);
        }

        /// <summary>
        /// Instructs the reader to order by specified records
        /// </summary>
        /// <param name="selector">The key to order by</param>
        public OrmResultSet<TData> OrderBy(Expression<Func<TData, dynamic>> keySelector)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().OrderBy<TData>(keySelector, Core.Model.Map.SortOrderType.OrderBy));
        }

        /// <summary>
        /// Instructs the reader to order by specified records
        /// </summary>
        /// <param name="selector">The selector to order by </param>
        public OrmResultSet<TData> OrderByDescending(Expression<Func<TData, dynamic>> keySelector)
        {
            return new OrmResultSet<TData>(this.Context, this.Statement.Build().OrderBy<TData>(keySelector, Core.Model.Map.SortOrderType.OrderByDescending));
        }

        /// <summary>
        /// Return the first object
        /// </summary>
        public TData First()
        {
            TData retVal = this.FirstOrDefault();
            if (retVal == null)
                throw new InvalidOperationException("Sequence contains no elements");
            return retVal;
        }

        /// <summary>
        /// Return the first object
        /// </summary>
        public TData FirstOrDefault()
        {
            return this.Context.FirstOrDefault<TData>(this.Statement);
        }

        /// <summary>
        /// Get the enumerator
        /// </summary>
        public IEnumerator<TData> GetEnumerator()
        {
            return this.Context.ExecQuery<TData>(this.Statement).GetEnumerator();
        }

        /// <summary>
        /// Get the non-generic enumerator
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.Context.ExecQuery<TData>(this.Statement).GetEnumerator();
        }

        /// <summary>
        /// Gets the specified keys from the object
        /// </summary>
        public OrmResultSet<T> Keys<T>(bool qualifyKeyTableName = true)
        {

            var innerQuery = this.Statement.Build();
            var tm = TableMapping.Get(typeof(TData));
            if (tm.TableName.StartsWith("CompositeResult"))
            {
                tm = TableMapping.Get(typeof(TData).GetGenericArguments().Last());
            }
            if (tm.PrimaryKey.Count() != 1)
                throw new InvalidOperationException("Cannot execute KEY query on object with no keys");

            // HACK: Swap out SELECT * if query starts with it
            if (innerQuery.SQL.StartsWith("SELECT * "))
            {
                if (qualifyKeyTableName)
                    innerQuery = this.Context.CreateSqlStatement($"SELECT {tm.TableName}.{tm.PrimaryKey.First().Name} {innerQuery.SQL.Substring(9)}", innerQuery.Arguments.ToArray());
                else
                    innerQuery = this.Context.CreateSqlStatement($"SELECT {tm.PrimaryKey.First().Name} {innerQuery.SQL.Substring(9)}", innerQuery.Arguments.ToArray());
            }

            return new OrmResultSet<T>(this.Context, this.Context.CreateSqlStatement($"SELECT {String.Join(",", tm.PrimaryKey.Select(o => o.Name))} FROM (").Append(innerQuery).Append(") AS I"));

        }

        /// <summary>
        /// Gets the specified keys from the specified object type
        /// </summary>
        public IEnumerable<T> Keys<TKeyTable, T>()
        {
            var innerQuery = this.Statement.Build();
            var tm = TableMapping.Get(typeof(TKeyTable));

            if (tm.PrimaryKey.Count() != 1)
                throw new InvalidOperationException("Cannot execute KEY query on object with no keys");

            // HACK: Swap out SELECT * if query starts with it
            if (innerQuery.SQL.StartsWith("SELECT * "))
                innerQuery = this.Context.CreateSqlStatement($"SELECT {tm.TableName}.{tm.PrimaryKey.First().Name} {innerQuery.SQL.Substring(9)}", innerQuery.Arguments.ToArray());


            return new OrmResultSet<T>(this.Context, this.Context.CreateSqlStatement($"SELECT {String.Join(",", tm.PrimaryKey.Select(o => o.Name))} FROM (").Append(innerQuery).Append(") AS I"));
        }

        /// <summary>
        /// Gets the keys
        /// </summary>
        IOrmResultSet IOrmResultSet.Keys<TKey>()
        {
            return this.Keys<TKey>(false);
        }

        /// <summary>
        /// Gets the keys
        /// </summary>
        IOrmResultSet IOrmResultSet.Skip(int n)
        {
            return this.Skip(n);
        }

        /// <summary>
        /// Gets the keys
        /// </summary>
        IOrmResultSet IOrmResultSet.Take(int n)
        {
            return this.Take(n);
        }

        /// <summary>
        /// Convert to an SQL statement
        /// </summary>
        public SqlStatement ToSqlStatement()
        {
            return this.Statement.Build();
        }

    }
}
