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
    public class IQueryableTest
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
        /// Test that a simple query can be executed
        /// </summary>
        [TestMethod]
        public void TestCanDoSimpleQuery()
        {
            using(var context = this.m_provider.GetReadonlyConnection())
            {
                context.Open();

                var systemQuery = context.Query<DbSecurityPolicy>(o => o.Name == "Login");
                Assert.AreEqual(1, systemQuery.Count());

            }
        }


        /// <summary>
        /// Test that a simple query can be executed
        /// </summary>
        [TestMethod]
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
        [TestMethod]
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
        [TestMethod]
        public void TestCanEnumerateResults()
        {
            using (var context = this.m_provider.GetReadonlyConnection())
            {
                context.Open();

                var systemQuery = context.Query<DbSecurityPolicy>(o => o.CreationTime > DateTime.MinValue);
                foreach (var result in systemQuery)
                    Assert.IsNotNull(result);

                foreach (var result in systemQuery.Skip(10).Take(10))
                    Assert.IsNotNull(result);
            }
        }
    }
}
