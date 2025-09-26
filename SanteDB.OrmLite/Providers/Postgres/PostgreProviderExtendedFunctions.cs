using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text;

namespace SanteDB.OrmLite.Providers.Postgres
{
    /// <summary>
    /// A class to reflect extended functions out of the Postgres provider which are not part of the data adapter interfaces.
    /// </summary>
    internal class PostgreProviderExtendedFunctions
    {
        public PostgreProviderExtendedFunctions(IDbConnection provider):
            this(provider?.GetType() ?? throw new NullReferenceException("Provider is null."))
        {

        }

        public PostgreProviderExtendedFunctions(Type providerType)
        {
            IsSupported = true; //Presume we are supported unless a method is not found.

            {
                var pi = providerType.GetProperty("PostgreSqlVersion", BindingFlags.Public | BindingFlags.Instance);

                if (null != pi && pi.PropertyType == typeof(Version))
                    PostgreSqlVersion = conn => (Version)pi.GetValue(conn);
                else
                {
                    PostgreSqlVersion = conn => throw new NotSupportedException("Provider does not support PostgreSqlVersion function.");
                    IsSupported = false;
                }

            }

            {
                var mi = providerType.GetMethod("BeginRawBinaryCopy", BindingFlags.Public | BindingFlags.Instance);

                if (null != mi && typeof(Stream).IsAssignableFrom(mi.ReturnType))
                    BeginRawBinaryCopy = (conn, sql) => (Stream)mi.Invoke(conn, new[] { sql });
                else
                {
                    BeginRawBinaryCopy = (conn, sql) => throw new NotSupportedException("Provider does not support BeginRawBinaryCopy function.");
                    IsSupported = false;
                }

            }

            var transactiontype = providerType.GetMethod("BeginTransaction", Type.EmptyTypes)?.ReturnType;

            if (null != transactiontype)
            {
                {
                    var mi = transactiontype.GetMethod("Save", new[] { typeof(string) });

                    if (null != mi)
                        TransactionSavepointSave = (conn, name) => mi.Invoke(conn, new[] { name });
                    else
                    {
                        TransactionSavepointSave = (conn, name) => throw new NotSupportedException("Provider does not support Save function on transactions.");
                        IsSupported = false;
                    }
                }

                {
                    var mi = transactiontype.GetMethod("Rollback", new[] { typeof(string) });

                    if (null != mi)
                        TransactionSavepointRollback = (conn, name) => mi.Invoke(conn, new[] { name });
                    else
                    {
                        TransactionSavepointRollback = (conn, name) => throw new NotSupportedException("Provider does not support Rollback function on transaction.");
                        IsSupported = false;
                    }
                }
            }
            else
            {
                IsSupported = false;
            }
        }

        /// <summary>
        /// Gets a func which invokes the PostgreSqlVersion property accessor on an NpgsqlConnection
        /// </summary>
        public Func<IDbConnection, Version> PostgreSqlVersion { get; private set; }

        /// <summary>
        /// Gets a func which invokes the BeginRawBinaryCopy method with the given SQL statement on an NpgsqlConnection.
        /// </summary>
        public Func<IDbConnection, string, Stream> BeginRawBinaryCopy { get; private set; }

        /// <summary>
        /// Gets a func which invokes the Save method with the given savepoint name on an NpgsqlTransaction.
        /// </summary>
        public Action<IDbTransaction, string> TransactionSavepointSave { get; private set; }

        /// <summary>
        /// Gets a func which invokes the Rollback method with the given savepoint name on an NpgsqlTransaction.
        /// </summary>
        public Action<IDbTransaction, string> TransactionSavepointRollback { get; private set; }

        public bool IsSupported { get; }
    }
}
