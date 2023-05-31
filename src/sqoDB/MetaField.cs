using System;

namespace sqoDB
{
    /// <summary>
    ///     Class that describe a field of an object stored in database
    /// </summary>
    public class MetaField
    {
        /// <summary>
        ///     Name of field stored in database
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        ///     Type of field stored in database
        /// </summary>
        public Type FieldType { get; set; }
    }
}