/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-3-10
 */
using NUnit.Framework;
using SanteDB.OrmLite.Providers.Firebird;
using SanteDB.OrmLite.Providers.Sqlite;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;
using System;
using System.Configuration;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace SanteDB.OrmLite.Tests
{
    [ExcludeFromCodeCoverage]
    [TestFixture(Category = "ORM")]
    public class IQueryableTest
    {

        // Provider for unit tests
        private SqliteProvider m_provider = new SqliteProvider() { ConnectionString = ConfigurationManager.ConnectionStrings["TEST_CONNECTION"].ConnectionString };

        /// <summary>
        /// Setup test
        /// </summary>
        [SetUp]
        public void ClassSetup()
        {
            var sql = new SqlStatement("foo");
            AppDomain.CurrentDomain.SetData(
              "DataDirectory",
              Path.Combine(TestContext.CurrentContext.TestDirectory, string.Empty));
        }

        /// <summary>
        /// Test that a simple query can be executed
        /// </summary>
        [Test]
        public void TestCanDoSimpleQuery()
        {
            using (var context = this.m_provider.GetReadonlyConnection())
            {
                context.Open();

                var systemQuery = context.Query<DbSecurityPolicy>(o => o.Name == "Login");
                Assert.AreEqual(1, systemQuery.Count());

            }
        }

        
        /// <summary>
        /// Test that a simple query can be executed
        /// </summary>
        [Test]
        public void TestCanLimitOffset()
        {
            using (var context = this.m_provider.GetReadonlyConnection())
            {
                context.Open();

                var systemQuery = context.Query<DbSecurityPolicy>(o => o.CreationTime > DateTime.MinValue);
                var count = systemQuery.Count();

                // Now we want to offset
                systemQuery = systemQuery.Skip(5);
                Assert.AreEqual(count - 5, systemQuery.Count());

                // Now we want to limit
                systemQuery = systemQuery.Take(3);
                Assert.AreEqual(3, systemQuery.Count());
            }
        }


        /// <summary>
        /// Test that ordering works
        /// </summary>
        [Test]
        public void TestCanOrderBy()
        {
            using (var context = this.m_provider.GetReadonlyConnection())
            {
                context.Open();

                var systemQuery = context.Query<DbSecurityPolicy>(o => o.CreationTime > DateTime.MinValue);
                // Executes LIMIT 1 Query 
                var first = systemQuery.First();

                // Now we want to sort by name
                var nameFirst = systemQuery.OrderBy(o => o.Name).First();

                // Now we want to sort descending name
                var nameReverse = systemQuery.OrderByDescending(o => o.Name).First();

                Assert.AreNotEqual(first.Key, nameFirst.Key);
                Assert.AreNotEqual(first.Key, nameReverse.Key);
                Assert.AreNotEqual(nameFirst.Key, nameReverse.Key);

            }
        }


        /// <summary>
        /// Test that enumeration works
        /// </summary>
        [Test]
        public void TestCanEnumerateResults()
        {
            using (var context = this.m_provider.GetReadonlyConnection())
            {
                context.Open();

                var systemQuery = context.Query<DbSecurityPolicy>(o => o.CreationTime > DateTime.MinValue);
                foreach (var result in systemQuery)
                {
                    Assert.IsNotNull(result);
                }

                foreach (var result in systemQuery.Skip(10).Take(10))
                {
                    Assert.IsNotNull(result);
                }
            }
        }
    }
}
