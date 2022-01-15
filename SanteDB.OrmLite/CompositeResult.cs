using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// Multi type result used when a result set is a join
    /// </summary>
    /// <remarks>A composite result is used when the caller is joining together data 
    /// from multiple tables and would like the ORM result engine to load multiple 
    /// types of objects from a single tuple</remarks>
    /// <example>
    /// <code language="cs">
    ///     var sql = context.Provider.CreateSqlStatement&lt;Table1>().SelectFrom(typeof(Table1), typeof(Table2))
    ///         .InnerJoin&lt;Table1, Table2>(o=>o.ForeignKey, o=>o.PrimaryKey);
    ///     var results = context.Query&lt;CompositeResult&lt;Table1, Table2>>(sql);
    /// </code>
    /// </example>
    public abstract class CompositeResult
    {

        /// <summary>
        /// Gets or sets the values
        /// </summary>
        public Object[] Values { get; protected set; }

        /// <summary>
        /// Parse values form the open <paramref name="rdr"/> using the <paramref name="provider"/> to populate this <see cref="CompositeResult"/>
        /// </summary>
        /// <param name="rdr">The reader which is being read (the current row in the data reader)</param>
        /// <param name="provider">The database provider to use to convert data from the <paramref name="rdr"/></param>
        public abstract void ParseValues(IDataReader rdr, IDbProvider provider);

        /// <summary>
        /// Parse the data
        /// </summary>
        protected TData Parse<TData>(IDataReader rdr, IDbProvider provider)
        {
            var tableMapping = TableMapping.Get(typeof(TData));
            dynamic result = Activator.CreateInstance(typeof(TData));
            // Read each column and pull from reader
            foreach (var itm in tableMapping.Columns)
            {
                try
                {
                    object value = provider.ConvertValue(rdr[itm.Name], itm.SourceProperty.PropertyType);
                    itm.SourceProperty.SetValue(result, value);
                }
                catch
                {
                    throw new MissingFieldException(tableMapping.TableName, itm.Name);
                }
            }
            return result;
        }
    }


    /// <summary>
    /// Multi-type result for two types
    /// </summary>
    public class CompositeResult<TData1, TData2> : CompositeResult
    {

        /// <summary>
        /// Gets the first object in the composite result
        /// </summary>
        public TData1 Object1 { get { return (TData1)this.Values[0]; } }

        /// <summary>
        /// Gets the second object in the composite result
        /// </summary>
        public TData2 Object2 { get { return (TData2)this.Values[1]; } }

        /// <inheritdoc/>
        public override void ParseValues(IDataReader rdr, IDbProvider provider)
        {
            this.Values = new object[] { this.Parse<TData1>(rdr, provider), this.Parse<TData2>(rdr, provider) };
        }
    }

    /// <summary>
    /// Multi-type result for three types
    /// </summary>
    public class CompositeResult<TData1, TData2, TData3> : CompositeResult<TData1, TData2>
    {
        /// <summary>
        /// Gets the third object in the composite result
        /// </summary>
        public TData3 Object3 { get { return (TData3)this.Values[2]; } }

        /// <inheritdoc/>
        public override void ParseValues(IDataReader rdr, IDbProvider provider)
        {
            this.Values = new object[] { this.Parse<TData1>(rdr, provider), this.Parse<TData2>(rdr, provider), this.Parse<TData3>(rdr, provider) };
        }
    }

    /// <summary>
    /// Multi-type result for four types
    /// </summary>
    public class CompositeResult<TData1, TData2, TData3, TData4> : CompositeResult<TData1, TData2, TData3>
    {
        /// <summary>
        /// Gets the fourth object in the coposite result
        /// </summary>
        public TData4 Object4 { get { return (TData4)this.Values[3]; } }

        /// <inheritdoc/>
        public override void ParseValues(IDataReader rdr, IDbProvider provider)
        {
            this.Values = new object[] { this.Parse<TData1>(rdr, provider), this.Parse<TData2>(rdr, provider), this.Parse<TData3>(rdr, provider), this.Parse<TData4>(rdr, provider) };
        }
    }

}
