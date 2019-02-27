using SanteDB.Core.Configuration.Data;
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
                            return (SqlFeature)null;
                        }
                    })).OfType<IDataFeature>().ToList();
            return m_features.Where(o=>o.InvariantName == invariantName);
        }


    }
}
