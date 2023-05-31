using System;
using System.Collections.Generic;
using sqoDB.Meta;
#if ASYNC
using System.Threading.Tasks;
#endif

namespace sqoDB.Queries
{
    internal class Where : ICriteria
    {
        public Where(string fieldName, OperationType opType, object val)
        {
            AttributeName.Add(fieldName);
            this.OperationType = opType;
            Value = val;
        }

        public Where()
        {
        }

        public List<string> AttributeName { get; set; } = new List<string>();

        public List<Type> ParentType { get; set; } = new List<Type>();

        public SqoTypeInfo ParentSqoTypeInfo { get; set; }

        public object Value { get; set; }

        public object Value2 { get; set; }

        public OperationType OperationType { get; set; }

        public StorageEngine StorageEngine { get; set; }

        #region ICriteria Members

        public List<int> GetOIDs()
        {
            var oids = StorageEngine.LoadFilteredOids(this);

            return oids;
        }

        #endregion

#if ASYNC
        public async Task<List<int>> GetOIDsAsync()
        {
            var oids = await StorageEngine.LoadFilteredOidsAsync(this).ConfigureAwait(false);

            return oids;
        }
#endif
    }

    internal enum OperationType
    {
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
        StartWith,
        EndWith,
        Contains,
        ContainsKey,
        ContainsValue
    }
}