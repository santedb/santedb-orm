using System;

namespace SanteDB.OrmLite.Providers
{

    /// <summary>
    /// States of the database statement
    /// </summary>
    public enum DbStatementStatus
    {
        Idle,
        Active,
        Stalled,
        Other
    }

    /// <summary>
    /// Represents a single database status record
    /// </summary>
    public class DbStatementReport
    {

        /// <summary>
        /// Gets the statement identifier
        /// </summary>
        public string StatementId { get; set; }

        /// <summary>
        /// Gets the current state of the action
        /// </summary>
        public DbStatementStatus Status { get; set; }

        /// <summary>
        /// Gets the current query
        /// </summary>
        public string Query { get; set; }

        /// <summary>
        /// Gets the start time
        /// </summary>
        public DateTime Start { get; set; }

    }
}