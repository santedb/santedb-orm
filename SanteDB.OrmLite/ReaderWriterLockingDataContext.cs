/*
 * Copyright (C) 2021 - 2025, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2024-6-21
 */
using SanteDB.Core.i18n;
using SanteDB.OrmLite.Diagnostics;
using SanteDB.OrmLite.Providers;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;

namespace SanteDB.OrmLite
{
    /// <summary>
    /// A specialized data context that supports locking the provider to a single writable connection and multiple read connections concurrently.
    /// </summary>
    public class ReaderWriterLockingDataContext : DataContext
    {

        /// <summary>
        /// READ lock timeout
        /// </summary>
        public const int READ_LOCK_TIMEOUT = 15_000;
        /// <summary>
        /// Writer lock timeout
        /// </summary>
        public const int WRITE_LOCK_TIMEOUT = 2000;

        private readonly string m_databaseName;
        /// <summary>
        /// The global lock object that is used to control access to the provider.
        /// </summary>
        private static readonly ConcurrentDictionary<String, ReaderWriterLockSlim> s_Locks = new ConcurrentDictionary<String, ReaderWriterLockSlim>();

        /// <summary>
        /// Gets a reference to the lock for a specific <paramref name="provider"/> or creates one if it does not exist.
        /// </summary>
        /// <param name="databaseName">The provider to use as the key for checking the dictionary with.</param>
        /// <returns>An instance of <see cref="ReaderWriterLockSlim"/> scoped to the <paramref name="provider"/>.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <see cref="ConcurrentDictionary{TKey, TValue}.TryGetValue(TKey, out TValue)"/> and <see cref="ConcurrentDictionary{TKey, TValue}.TryAdd(TKey, TValue)"/> both return false indicating we cannot add to the dictionary but the key does not exist.</exception>
        protected static ReaderWriterLockSlim GetLock(string databaseName)
        {
            if (!s_Locks.TryGetValue(databaseName, out var lck))
            {
                //We support recursion for the case of cloning a write context. Not safe but need to be fixed by the caller, not us.
                lck = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

                if (!s_Locks.TryAdd(databaseName, lck))
                {
                    lck.Dispose();
                    if (!s_Locks.TryGetValue(databaseName, out lck))
                    {
                        throw new InvalidOperationException("Failed to get lock after failing to add lock to dictionary. This usually indicates a serious thread synchronization problem.");
                    }
                }
            }

            return lck;
        }

        /// <summary>
        /// Creates a new data context.
        /// </summary>
        /// <param name="provider">The underlying provider for this context.</param>
        /// <param name="connection">The connection created by the provider for this context.</param>
        /// <remarks>This constructor will mark the data context as writable and lock the provider.</remarks>
        public ReaderWriterLockingDataContext(IDbProvider provider, IDbConnection connection) : base(provider, connection)
        {
            this.m_databaseName = provider.GetDatabaseName();
        }

        /// <summary>
        /// Creates a new data context, and optionally marks it read only.
        /// </summary>
        /// <param name="provider">The underlying provider for this context.</param>
        /// <param name="connection">The connection created by the provider for this context.</param>
        /// <param name="isReadonly">True to mark the connection as read-only. Multiple read-only contexts can execute simultaneously. False to mark the connection writable. Only one writable context can execute simultaneously.</param>
        public ReaderWriterLockingDataContext(IDbProvider provider, IDbConnection connection, bool isReadonly) : base(provider, connection, isReadonly)
        {
            this.m_databaseName = provider.GetDatabaseName();
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
            this.m_databaseName = provider.GetDatabaseName();
        }

        /// <inheritdoc />
        public override bool Open()
        {
            var ormProbe = this.Provider is IDbMonitorProvider monitorProvider ? monitorProvider.MonitorProbe as OrmClientProbe : null;

            // Get the lock 
            try
            {
                ormProbe?.Increment(OrmPerformanceMetric.AwaitingLock);
                var locker = GetLock(this.m_databaseName);
                if (this.IsReadonly)
                {
                    if (!locker.TryEnterReadLock(READ_LOCK_TIMEOUT))
                    {
                        throw new InvalidOperationException(ErrorMessages.READ_LOCK_UNAVAILABLE);
                    }
                }
                else
                {
                    // Release our read lock and attempt to get a write lock
                    while (locker.IsReadLockHeld)
                    {
                        locker.ExitReadLock();
                    }
                    if (!locker.TryEnterWriteLock(WRITE_LOCK_TIMEOUT))
                    {
                        throw new InvalidOperationException(ErrorMessages.WRITE_LOCK_UNAVAILABLE);
                    }
                }


                if (!base.Open())
                {
                    if (locker.IsReadLockHeld)
                    {
                        locker.ExitReadLock();
                    }
                    if (locker.IsWriteLockHeld)
                    {
                        locker.ExitWriteLock();
                    }
                    return false;
                }

                
                return true;
            }
            finally
            {
                ormProbe?.Decrement(OrmPerformanceMetric.AwaitingLock);
            }
        }

        /// <summary>
        /// Prevent all connections to the database 
        /// </summary>
        internal void Lock()
        {
            var locker = GetLock(this.m_databaseName);
            locker.EnterWriteLock(); // block here until we get a lock - this will prevent all other connection attempts
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
        private void EnsureLockRelease(bool releaseAll = false)
        {
            var locker = GetLock(this.m_databaseName);
            while(locker.IsReadLockHeld)
            {
                locker.ExitReadLock();
                if (!releaseAll) break;
            }
            while(locker.IsWriteLockHeld)
            {
                locker.ExitWriteLock();
                if (!releaseAll) break;
            }
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
                
                this.EnsureLockRelease(releaseAll: true);
            }
        }

    }
}
