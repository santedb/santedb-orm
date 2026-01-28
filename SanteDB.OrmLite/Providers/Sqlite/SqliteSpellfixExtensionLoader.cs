/*
 * Copyright (C) 2021 - 2026, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
using SanteDB.Core.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SanteDB.OrmLite.Providers.Sqlite
{
    /// <summary>
    /// This static class helps load the spellfix library into a connection if it does not already exist.
    /// </summary>
    public static class SqliteSpellfixExtensionLoader
    {
        private static string s_LibraryName;
        private static string s_EntryPoint;
        private static bool? s_SpellfixLoaded;
        private static readonly Tracer s_Tracer;

        static SqliteSpellfixExtensionLoader()
        {
            s_Tracer = Tracer.GetTracer(typeof(SqliteSpellfixExtensionLoader));
        }

        /// <summary>
        /// Sets the library name and optionally entry point to load the spellfix extension into the sqlite instance in memory.
        /// </summary>
        /// <param name="libraryName">The library name that contains the spellfix library.</param>
        /// <param name="entryPoint">Optinally the entry point to use when loading the spellfix library. If this is null, the default entry point in sqlite is used.</param>
        public static void SetLibraryInformation(string libraryName, string entryPoint = null)
        {
            s_LibraryName = libraryName;
            s_EntryPoint = entryPoint;
            s_SpellfixLoaded = null;
        }

        /// <summary>
        /// Checks if spellfix has been loaded, and if not, attemps to load it using the values from <see cref="SetLibraryInformation(string, string)"/>.
        /// </summary>
        /// <param name="connection">A connection object to check if the library has been loaded with.</param>
        /// <returns>True if the library has been loaded, false if it could not be loaded.</returns>
        public static bool CheckAndLoadSpellfix(this IDbConnection connection)
        {

            //try
            //{
            //    //Check if spellfix is loaded by executing a function.
            //    s_SpellfixLoaded = connection.ExecuteScalar<int>("SELECT editdist3('test', 'test1');") > 0;
            //    s_Tracer?.TraceVerbose("Test succeeded for loading spellfix library for sqlite. Result: {0}", s_SpellfixLoaded);
            //    return true;
            //}
            //catch //An exception is thrown if the function is not found.
            //{

            //    if (null != s_LibraryName) //If we've been provided with a specific library to load, use that.
            //    {
            //        s_Tracer.TraceVerbose("Attempting to load spellfix library using '{0}', entry point '{1}'.", s_LibraryName, s_EntryPoint ?? "(default)");

            //        try
            //        {
            //            connection.LoadExtension(s_LibraryName, s_EntryPoint);
            //        }
            //        catch(Exception ex)
            //        {
            //            if (ex.Message?.IndexOf("sqlite error 1", StringComparison.OrdinalIgnoreCase) > -1)
            //            {
            //                string errormessage = null;

            //                if (ex.Message?.IndexOf("procedure", StringComparison.OrdinalIgnoreCase) > -1)
            //                {
            //                    errormessage = "Entry point not found when attempting to load spellfix library using '{0}', entry point '{1}'.";
            //                }
            //                else if (ex.Message?.IndexOf("module", StringComparison.OrdinalIgnoreCase) > -1)
            //                {
            //                    errormessage = "Library not found when attempting to load spellfix library using '{0}', entry point '{1}'.";
            //                }
            //                else
            //                {
            //                    errormessage = "Unspecified error when attempting to load spellfix library using '{0}', entry point '{1}'.";
            //                }

            //                if (null != errormessage) 
            //                    s_Tracer.TraceError(errormessage, s_LibraryName, s_EntryPoint ?? "(default)");
            //            }
            //            throw;
            //        }
            //    }
            //    else //Otherwise use the default name for the extension library.
            //    {
            try
            {
                try
                {
                    connection.Execute("SELECT editdist3('test', 'test1');");
                }
                catch
                {
                    if (s_LibraryName == null)
                    {
                        connection.LoadExtension("e_sqlite3mc", "sqlite3_spellfix_init");
                    }
                    else
                    {
                        connection.LoadExtension(s_LibraryName, s_EntryPoint ?? "sqlite3_spellfix_init");
                    }
                }
            }
            catch
            {
                s_Tracer?.TraceError("Failed to load spellfix library using default module name '{0}'.", "spellfix");
            }

            //}

            try
            {
                s_SpellfixLoaded = connection.ExecuteScalar<int>("SELECT editdist3('test', 'test1');") > 0;
            }
            catch
            {
                s_SpellfixLoaded = false;
            }
            //}

            return s_SpellfixLoaded == true;
        }

    }
}
