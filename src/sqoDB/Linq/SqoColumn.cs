using System;

namespace sqoDB
{
    internal class SqoColumn
    {
        public Type SourceType { get; set; }

        public string SourcePropName { get; set; }

        public bool IsFullObject { get; set; }
    }
}