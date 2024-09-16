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
using SanteDB.Core.i18n;
using SanteDB.Core.Model.Query;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// ORM BI enumerator
    /// </summary>
    public class OrmQueryResultSet<TResult> : IEnumerable<TResult>, IOrderableQueryResultSet<TResult>
    {
        private readonly OrmResultSet<TResult> m_ormResultSet;

        /// <summary>
        /// Result set of the ORM enumerator
        /// </summary>
        public OrmQueryResultSet(IOrmResultSet ormResultSet)
        {
            this.m_ormResultSet = (OrmResultSet<TResult>)ormResultSet;
        }

        /// <inheritdoc/>
        public Type ElementType => typeof(ExpandoObject);

        /// <inheritdoc/>
        public bool Any() => this.m_ormResultSet.Any();

        /// <inheritdoc/>
        public IQueryResultSet<object> AsStateful(Guid stateId)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public int Count() => this.m_ormResultSet.Count();

        /// <inheritdoc/>
        public IQueryResultSet<TResult> Distinct() => new OrmQueryResultSet<TResult>(this.m_ormResultSet.Distinct());

        /// <inheritdoc/>
        public IQueryResultSet<TResult> Except(Expression<Func<TResult, bool>> query) => throw new NotSupportedException();

        /// <inheritdoc/>
        public object First()
        {
            var retVal = this.FirstOrDefault();
            if (retVal == null)
            {
                throw new InvalidOperationException(ErrorMessages.SEQUENCE_NO_ELEMENTS);
            }
            else
            {
                return retVal;
            }
        }

        /// <inheritdoc/>
        public object FirstOrDefault() => this.m_ormResultSet.FirstOrDefault();

        /// <inheritdoc/>
        public IEnumerator<TResult> GetEnumerator()
        {
            try
            {
                this.m_ormResultSet.Context.Open();
                foreach (var itm in this.m_ormResultSet)
                {
                    yield return itm;
                }
            }
            finally
            {
                this.m_ormResultSet.Context.Close();
            }
        }

        //{
        //    using (var context = this.m_ormResultSet.Context.OpenClonedContext())
        //    {
        //        context.Open();
        //        foreach (var itm in this.m_ormResultSet.CloneOnContext(context) as OrmResultSet<TResult>)
        //        {
        //            yield return itm;
        //        }
        //    }
        //}

        /// <inheritdoc/>
        public IQueryResultSet Intersect(IQueryResultSet other)
        {
            if (other is OrmQueryResultSet<TResult> obqs)
            {
                return new OrmQueryResultSet<TResult>(this.m_ormResultSet.Intersect(obqs.m_ormResultSet));
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(other), String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(OrmQueryResultSet<TResult>), other.GetType()));
            }
        }

        /// <inheritdoc/>
        public IQueryResultSet<TResult> Intersect(IQueryResultSet<TResult> other)
        {
            if (other is OrmQueryResultSet<TResult> otr)
            {
                return new OrmQueryResultSet<TResult>(this.m_ormResultSet.Intersect(otr.m_ormResultSet));
            }
            else
            {
                throw new ArgumentOutOfRangeException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(OrmQueryResultSet<TResult>), other.GetType()));
            }
        }

        /// <inheritdoc/>
        public IQueryResultSet<TResult> Intersect(Expression<Func<TResult, bool>> query) => throw new NotSupportedException();

        /// <inheritdoc/>
        public IEnumerable<TType> OfType<TType>()
        {
            foreach (var itm in this)
            {
                if (itm is TType t)
                {
                    yield return t;
                }
            }
        }

        /// <inheritdoc/>
        public IOrderableQueryResultSet OrderBy(Expression expression) => new OrmQueryResultSet<TResult>(this.m_ormResultSet.OrderBy(expression as LambdaExpression));

        /// <inheritdoc/>
        public IOrderableQueryResultSet<TResult> OrderBy<TKey>(Expression<Func<TResult, TKey>> sortExpression) => new OrmQueryResultSet<TResult>(this.m_ormResultSet.OrderBy(sortExpression));


        /// <inheritdoc/>
        public IOrderableQueryResultSet OrderByDescending(Expression expression) => new OrmQueryResultSet<TResult>(this.m_ormResultSet.OrderByDescending(expression as LambdaExpression));

        /// <inheritdoc/>
        public IOrderableQueryResultSet<TResult> OrderByDescending<TKey>(Expression<Func<TResult, TKey>> sortExpression) => new OrmQueryResultSet<TResult>(this.m_ormResultSet.OrderByDescending(sortExpression));

        /// <inheritdoc/>
        public IEnumerable<TReturn> Select<TReturn>(Expression selector)
        {
            if (selector is LambdaExpression le)
            {
                var comp = le.Compile();
                foreach (var itm in this)
                {
                    yield return (TReturn)comp.DynamicInvoke(itm);
                }
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(selector), String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(LambdaExpression), selector.GetType()));
            }
        }

        /// <inheritdoc/>
        public IEnumerable<TReturn> Select<TReturn>(Expression<Func<TResult, TReturn>> selector) => this.m_ormResultSet.Select(selector);

        /// <inheritdoc/>
        public object Single()
        {
            var retVal = this.SingleOrDefault();
            if (retVal == null)
            {
                throw new InvalidOperationException(ErrorMessages.SEQUENCE_NO_ELEMENTS);
            }
            return retVal;
        }

        /// <inheritdoc/>
        public object SingleOrDefault()
        {
            if (this.m_ormResultSet.Count() > 1)
            {
                throw new InvalidOperationException(ErrorMessages.SEQUENCE_MORE_THAN_ONE);
            }
            else
            {
                return m_ormResultSet.FirstOrDefault();
            }
        }

        /// <inheritdoc/>
        public IQueryResultSet<TResult> Skip(int count) => new OrmQueryResultSet<TResult>(this.m_ormResultSet.Skip(count));

        /// <inheritdoc/>
        public IQueryResultSet<TResult> Take(int count) => new OrmQueryResultSet<TResult>(this.m_ormResultSet.Take(count));


        /// <inheritdoc/>
        public IQueryResultSet Union(IQueryResultSet other)
        {
            if (other is OrmQueryResultSet<TResult> ob)
            {
                return new OrmQueryResultSet<TResult>(this.m_ormResultSet.Union(ob.m_ormResultSet));
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(other), String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(OrmQueryResultSet<TResult>), other.GetType()));
            }
        }

        /// <inheritdoc/>
        public IQueryResultSet<TResult> Union(IQueryResultSet<TResult> other)
        {
            if (other is OrmQueryResultSet<TResult> otr)
            {
                return new OrmQueryResultSet<TResult>(this.m_ormResultSet.Union(otr.m_ormResultSet));
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(other), String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(OrmQueryResultSet<TResult>), other.GetType()));
            }
        }

        /// <inheritdoc/>
        public IQueryResultSet<TResult> Union(Expression<Func<TResult, bool>> query) => throw new NotSupportedException();

        /// <inheritdoc/>
        public IQueryResultSet Where(Expression query)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public IQueryResultSet<TResult> Where(Expression<Func<TResult, bool>> query)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        IQueryResultSet IQueryResultSet.AsStateful(Guid stateId)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        IQueryResultSet<TResult> IQueryResultSet<TResult>.AsStateful(Guid stateId)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        TResult IQueryResultSet<TResult>.First() => this.m_ormResultSet.First();

        /// <inheritdoc/>
        TResult IQueryResultSet<TResult>.FirstOrDefault() => this.m_ormResultSet.FirstOrDefault();

        /// <summary>
        /// Get enumerator
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();

        /// <inheritdoc/>
        TResult IQueryResultSet<TResult>.Single() => this.m_ormResultSet.Single();

        /// <inheritdoc/>
        TResult IQueryResultSet<TResult>.SingleOrDefault() => this.m_ormResultSet.SingleOrDefault();

        /// <inheritdoc/>
        IQueryResultSet IQueryResultSet.Skip(int count) => new OrmQueryResultSet<TResult>(this.m_ormResultSet.Skip(count));

        /// <inheritdoc/>
        IQueryResultSet IQueryResultSet.Take(int count) => new OrmQueryResultSet<TResult>(this.m_ormResultSet.Take(count));
    }
}