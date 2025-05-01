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
