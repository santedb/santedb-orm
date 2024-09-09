using SQLitePCL;
using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// Internal class to dynamically resolve the function addresses for the sqlite methods from a dynamically loaded instance.
    /// </summary>
    internal class SqliteFunctionLoader : SQLitePCL.IGetFunctionPointer
    {
        static readonly IntPtr _dll;

        static SqliteFunctionLoader()
        {
            //Resolve the location of the sqlite library.
            var assy = typeof(raw).Assembly;

            try
            {
                _dll = SqliteNativeMethods.Load("e_sqlite3mc", assy, SqliteNativeMethods.WHERE_RUNTIME_RID | SqliteNativeMethods.WHERE_ADJACENT);
            }
            catch
            {
                try // fallback to sqlciper
                {
                    _dll = SqliteNativeMethods.Load("e_sqlcipher", assy, SqliteNativeMethods.WHERE_RUNTIME_RID | SqliteNativeMethods.WHERE_ADJACENT);
                }
                catch
                {
                    throw new DllNotFoundException("Unable to locate sqlite database. Ensure the correct package is included in your project.");
                }
            }
        }

        public IntPtr GetFunctionPointer(string name)
        {
            if (SqliteNativeMethods.TryGetExport(_dll, name, out var f))
            {
                return f;
            }

            return IntPtr.Zero;
        }
    }
}
