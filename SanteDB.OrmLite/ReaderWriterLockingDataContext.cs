using SanteDB.Core.i18n;
using SanteDB.Core.Model.Roles;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// A specialized data context that supports locking the provider to a single writable connection and multiple read connections concurrently.
    /// </summary>
    public class ReaderWriterLockingDataContext : DataContext
    {

        /// <summary>
        /// The global lock object that is used to control access to the provider.
        /// </summary>
        private static readonly ConcurrentDictionary<IDbProvider, ReaderWriterLockSlim> s_Locks = new ConcurrentDictionary<IDbProvider, ReaderWriterLockSlim>();

        /// <summary>
        /// Gets a reference to the lock for a specific <paramref name="provider"/> or creates one if it does not exist.
        /// </summary>
        /// <param name="provider">The provider to use as the key for checking the dictionary with.</param>
        /// <returns>An instance of <see cref="ReaderWriterLockSlim"/> scoped to the <paramref name="provider"/>.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <see cref="ConcurrentDictionary{TKey, TValue}.TryGetValue(TKey, out TValue)"/> and <see cref="ConcurrentDictionary{TKey, TValue}.TryAdd(TKey, TValue)"/> both return false indicating we cannot add to the dictionary but the key does not exist.</exception>
        protected static ReaderWriterLockSlim GetLock(IDbProvider provider)
        {
            if (!s_Locks.TryGetValue(provider, out var lck))
            {
                //We support recursion for the case of cloning a write context. Not safe but need to be fixed by the caller, not us.
                lck = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion); 

                if (!s_Locks.TryAdd(provider, lck))
                {
                    lck.Dispose();
                    if (!s_Locks.TryGetValue(provider, out lck))
                    {
                        throw new InvalidOperationException("Failed to get lock after failing to add lock to dictionary. This usually indicates a serious thread synchronization problem.");
                    }
                }
            }

            return lck;
        }

        private ReaderWriterLockSlim _Lock;

        /// <summary>
        /// Creates a new data context.
        /// </summary>
        /// <param name="provider">The underlying provider for this context.</param>
        /// <param name="connection">The connection created by the provider for this context.</param>
        /// <remarks>This constructor will mark the data context as writable and lock the provider.</remarks>
        public ReaderWriterLockingDataContext(IDbProvider provider, IDbConnection connection) : base(provider, connection)
        {
            
        }

        /// <summary>
        /// Creates a new data context, and optionally marks it read only.
        /// </summary>
        /// <param name="provider">The underlying provider for this context.</param>
        /// <param name="connection">The connection created by the provider for this context.</param>
        /// <param name="isReadonly">True to mark the connection as read-only. Multiple read-only contexts can execute simultaneously. False to mark the connection writable. Only one writable context can execute simultaneously.</param>
        public ReaderWriterLockingDataContext(IDbProvider provider, IDbConnection connection, bool isReadonly) : base(provider, connection, isReadonly)
        {
           
        }

        /// <summary>
        /// Creates a new writable data context with an existing transaction.
        /// </summary>
        /// <param name="provider">The underling provider for this context.</param>
        /// <param name="connection">The connection created by the provider for this context.</param>
        /// <param name="tx">The transaction this context will be enlisted in.</param>
        /// <remarks>This constructor will mark the data context as writable and lock the provider.</remarks>
        public ReaderWriterLockingDataContext(IDbProvider provider, IDbConnection connection, IDbTransaction tx) : base(provider, connection, tx)
        {
            
        }
        /// <inheritdoc />
        public override bool Open()
        {
            if (base.Open())
            {
                _Lock = GetLock(this.Provider);
                if (IsReadonly)
                    _Lock.EnterReadLock();
                else if (!_Lock.TryEnterWriteLock(1000))
                    throw new InvalidOperationException(ErrorMessages.WRITE_LOCK_UNAVAILABLE);
                return true;
            }
            return false;
        }

        /// <inheritdoc/>
        public override void Close()
        {
            this.EnsureLockRelease();
            base.Close();
        }

        /// <summary>
        /// Ensure lock is released
        /// </summary>
        private void EnsureLockRelease()
        {
            if (this.IsReadonly && _Lock.IsReadLockHeld)
                _Lock.ExitReadLock();
            else if(!this.IsReadonly && _Lock.IsWriteLockHeld)
                _Lock.ExitWriteLock();
        }

        ///<inheritdoc />
        public override void Dispose()
        {
            try
            {
                base.Dispose();
            }
            finally
            {
                this.EnsureLockRelease();
            }
        }

    }
}
