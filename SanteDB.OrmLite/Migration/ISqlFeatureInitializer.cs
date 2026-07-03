using DocumentFormat.OpenXml.Drawing.Charts;
using DocumentFormat.OpenXml.Office.Word;
using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.OrmLite.Migration
{
    /// <summary>
    /// SQL feature initializer
    /// </summary>
    public interface ISqlFeatureInitializer
    {

        /// <summary>
        /// Before installing the SQL feature
        /// </summary>
        /// <param name="context">The context on which the feature is being applied</param>
        /// <returns>True if installation should proceed</returns>
        bool BeforeInstall(DataContext context);

        /// <summary>
        /// After installing the SQL feature
        /// </summary>
        /// <param name="context">The data context on which the feature has been installed</param>
        void AfterInstall(DataContext context);
    }
}
