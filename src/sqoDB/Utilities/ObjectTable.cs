using System;
using System.Collections.Generic;

namespace sqoDB.Utilities
{
    internal class ObjectTable
    {
        private Dictionary<string, Type> columnTypes = new Dictionary<string, Type>();

        public List<ObjectRow> Rows { get; } = new List<ObjectRow>();

        public Dictionary<string, int> Columns { get; } = new Dictionary<string, int>();

        public ObjectRow NewRow()
        {
            return new ObjectRow(this);
        }
    }
}