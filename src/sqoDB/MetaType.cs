using System.Collections.Generic;

namespace sqoDB
{
    /// <summary>
    ///     Class that describe Type of objects  stored in database
    /// </summary>
    public class MetaType
    {
        /// <summary>
        ///     Name of Type stored in database
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        ///     List of fields
        /// </summary>
        public List<MetaField> Fields { get; } = new List<MetaField>();

        public string FileName { get; set; }

        public int TypeID { get; set; }
    }
}