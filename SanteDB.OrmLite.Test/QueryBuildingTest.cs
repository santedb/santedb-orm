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
 * Date: 2023-5-19
 */
using NUnit.Framework;
using SanteDB.OrmLite.Providers.Sqlite;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;
using System;
using System.Configuration;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;

namespace SanteDB.OrmLite.Tests
{
    [ExcludeFromCodeCoverage]
    [TestFixture(Category = "ORM")]
    public class QueryBuildingTest
    {
        // Provider for unit tests
        private SqliteProvider m_provider = new SqliteProvider() { ConnectionString = ConfigurationManager.ConnectionStrings["TEST_CONNECTION"].ConnectionString };

        /// <summary>
        /// Setup test
        /// </summary>
        [SetUp]
        public void ClassSetup()
        {
            AppDomain.CurrentDomain.SetData(
              "DataDirectory",
              Path.Combine(TestContext.CurrentContext.TestDirectory, string.Empty));
        }

        /// <summary>
        /// Tests that the query builder can create an IN[] clause
        /// </summary>
        [Test]
        public void TestConstructsArrayContainerQuery()
        {
            using (var context = this.m_provider.GetReadonlyConnection())
            {
                context.Open();

                Guid[] uuids = new Guid[]
                {
                    Guid.NewGuid(),
                    Guid.NewGuid(),
                    Guid.NewGuid()
                };

                var systemQuery = context.Query<DbSecurityPolicy>(o => uuids.Contains(o.Key));
                Assert.AreEqual(0, systemQuery.Count());
            }
        }
    }
}