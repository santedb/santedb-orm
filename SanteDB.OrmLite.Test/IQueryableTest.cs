using System;
using System.Configuration;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using NUnit.Framework;
using SanteDB.OrmLite.Providers.Firebird;
using SanteDB.Persistence.Data.ADO.Data.Model.Security;

namespace SanteDB.OrmLite.Tests
{
    [ExcludeFromCodeCoverage]
    [TestFixture(Category = "ORM")]
    public class IQueryableTest
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
        /// Test that a simple query can be executed
        /// </summary>
        [Test]
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
                    Assert.IsNotNull(result);

                foreach (var result in systemQuery.Skip(10).Take(10))
                    Assert.IsNotNull(result);
            }
        }
    }
}
