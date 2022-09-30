/*
 * Copyright (C) 2021 - 2022, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2022-5-30
 */
using SanteDB.OrmLite.Providers;
using System;
using System.Data;

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
            where TData : new()
        {
            var tableMapping = TableMapping.Get(typeof(TData));
            var result = new TData();
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
        where TData1 : new()
        where TData2 : new()
    {

        public CompositeResult()
        {

        }

        /// <summary>
        /// Create composite result with specified values
        /// </summary>
        public CompositeResult(TData1 object1, TData2 object2)
        {
            this.Values[0] = object1;
            this.Values[1] = object2;
        }

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
                where TData1 : new()
        where TData2 : new()
        where TData3 : new()
    {

        public CompositeResult()
        {

        }

        /// <summary>
        /// Create composite result with specified values
        /// </summary>
        public CompositeResult(TData1 object1, TData2 object2, TData3 object3) : base(object1, object2)
        {
            this.Values[2] = object3;
        }

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
                where TData1 : new()
        where TData2 : new()
        where TData3 : new()
        where TData4 : new()
    {

        public CompositeResult()
        {

        }

        /// <summary>
        /// Create composite result with specified values
        /// </summary>
        public CompositeResult(TData1 object1, TData2 object2, TData3 object3, TData4 object4) : base(object1, object2, object3)
        {
            this.Values[3] = object4;
        }
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
