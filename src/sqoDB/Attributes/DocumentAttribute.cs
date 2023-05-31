using System;

namespace sqoDB.Attributes
{
    /// <summary>
    ///     Make property to be stored as a Document-a snapshot of current object.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class DocumentAttribute : Attribute
    {
    }
}