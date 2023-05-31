namespace sqoDB.Utilities
{
    internal class ATuple<T, V>
    {
        public ATuple(T name, V value)
        {
            Name = name;
            Value = value;
        }

        public T Name { get; set; }
        public V Value { get; set; }
    }
}