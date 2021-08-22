using SanteDB.OrmLite.Attributes;
using System;

namespace SanteDB.Persistence.Data.ADO.Data.Model.Error
{
    /// <summary>
    /// This class exists to be able to extract the function error code for functions which cannot raise an exception because
    /// they the transaction manager will rollback
    /// </summary>
    public class FunctionErrorCode
    {

        [Column("err_code")]
        public String ErrorCode { get; set; }
    }
}
