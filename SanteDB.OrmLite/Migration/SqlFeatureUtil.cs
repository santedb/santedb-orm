/*
 * Copyright 2015-2019 Mohawk College of Applied Arts and Technology
 * Copyright 2019-2019 SanteSuite Contributors (See NOTICE)
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
 * User: Justin Fyfe
 * Date: 2019-8-8
 */
using SanteDB.Core.Configuration.Data;
using SanteDB.Core.Diagnostics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.OrmLite.Migration
{
    /// <summary>
    /// Represents a utility class for deploying a SQL feature
    /// </summary>
    public static class SqlFeatureUtil 
    {

        // Features
        private static IEnumerable<IDataFeature> m_features = null;

        private static Tracer m_traceSource = Tracer.GetTracer(typeof(SqlFeatureUtil));

        /// <summary>
        /// Load the available features
        /// </summary>
        public static IEnumerable<IDataFeature> GetFeatures(String invariantName)
        {
            if (m_features == null)
                m_features = AppDomain.CurrentDomain.GetAssemblies()
                    .Where(a => !a.IsDynamic)
                    .SelectMany(a => a.GetManifestResourceNames().Where(n => n.ToLower().EndsWith(".sql")).Select(n =>
                    {
                        try
                        {
                            var retVal = SqlFeature.Load(a.GetManifestResourceStream(n));
                            retVal.Scope = retVal.Scope ?? a.FullName;
                            return retVal;
                        }
                        catch (Exception e){
                            m_traceSource.TraceError("Could not load {0}: {1}", n, e);
                            return (SqlFeature)null;
                        }
                    })).OfType<IDataFeature>().ToList();
            return m_features.Where(o=>o.InvariantName == invariantName);
        }


    }
}
