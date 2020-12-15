using System;
using System.Configuration;
using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SanteDB.OrmLite.Providers.Firebird;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;

namespace SanteDB.OrmLite.Test
{
    [TestClass]
    [DeploymentItem(@"santedb_test.fdb")]
    [DeploymentItem(@"fbclient.dll")]
    [DeploymentItem(@"firebird.conf")]
    [DeploymentItem(@"firebird.msg")]
    [DeploymentItem(@"ib_util.dll")]
    [DeploymentItem(@"icudt52.dll")]
    [DeploymentItem(@"icudt52l.dat")]
    [DeploymentItem(@"icuin52.dll")]
    [DeploymentItem(@"icuuc52.dll")]
    [DeploymentItem(@"plugins\engine12.dll", "plugins")]
    [DeploymentItem(@"FirebirdSql.Data.FirebirdClient.dll")]
    public class QueryBuildingTest
    {
        // Provider for unit tests
        private FirebirdSQLProvider m_provider = new FirebirdSQLProvider() { ConnectionString = ConfigurationManager.ConnectionStrings["TEST_CONNECTION"].ConnectionString };

        /// <summary>
        /// Setup test
        /// </summary>
        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            AppDomain.CurrentDomain.SetData(
              "DataDirectory",
              Path.Combine(context.DeploymentDirectory, string.Empty));
        }

        /// <summary>
        /// Tests that the query builder can create an IN[] clause
        /// </summary>
        [TestMethod]
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
