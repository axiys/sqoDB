namespace sqoDB.Utilities
{
    internal class ObjectRow
    {
        private readonly ObjectTable table;
        internal object[] cells;

        public ObjectRow(ObjectTable table)
        {
            this.table = table;
            cells = new object[table.Columns.Count];
        }

        public object this[string name]
        {
            get => cells[table.Columns[name]];
            set => cells[table.Columns[name]] = value;
        }

        public object this[int index]
        {
            get => cells[index];
            set => cells[index] = value;
        }
    }
}