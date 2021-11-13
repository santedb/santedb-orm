using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using NUnit.Framework;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.Entities;
using SanteDB.OrmLite.Providers.Firebird;
using SanteDB.Persistence.Data.ADO.Data.Model.DataType;
using SanteDB.Persistence.Data.ADO.Data.Model.Entities;
using SanteDB.Persistence.Data.ADO.Data.Model.Roles;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;

namespace SanteDB.OrmLite.Tests
{
    [ExcludeFromCodeCoverage]
    [TestFixture(Category = "ORM")]
    //[DeploymentItem(@"santedb_test.fdb")]
    //[DeploymentItem(@"fbclient.dll")]
    //[DeploymentItem(@"firebird.conf")]
    //[DeploymentItem(@"firebird.msg")]
    //[DeploymentItem(@"ib_util.dll")]
    //[DeploymentItem(@"icudt52.dll")]
    //[DeploymentItem(@"icudt52l.dat")]
    //[DeploymentItem(@"icuin52.dll")]
    //[DeploymentItem(@"icuuc52.dll")]
    //[DeploymentItem(@"plugins\engine12.dll", "plugins")]
    //[DeploymentItem(@"FirebirdSql.Data.FirebirdClient.dll")]
    public class QueryBuildingTest
    {
        // Provider for unit tests
        private FirebirdSQLProvider m_provider = new FirebirdSQLProvider() { ConnectionString = ConfigurationManager.ConnectionStrings["TEST_CONNECTION"].ConnectionString };

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
