﻿using SanteDB.OrmLite;
using SanteDB.OrmLite.Attributes;
using System;
using System.Diagnostics.CodeAnalysis;

namespace SanteDB.Persistence.Data.ADO.Data.Model
{
    /// <summary>
    /// Represents identified data
    /// </summary>

    public interface IDbIdentified
    {
        /// <summary>
        /// Gets or sets the key of the object
        /// </summary>
        Guid Key { get; set; }
    }

    /// <summary>
    /// Gets or sets the identified data
    /// </summary>
    [ExcludeFromCodeCoverage]
    public abstract class DbIdentified : IDbIdentified
    {
        /// <summary>
        /// Create database identified
        /// </summary>
        public DataContext Context { get; set; }

        /// <summary>
        /// Gets or sets the key of the object
        /// </summary>
        [AutoGenerated]
        public abstract Guid Key { get; set; }
    }

    
}
