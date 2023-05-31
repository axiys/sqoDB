using System;

namespace sqoDB.Attributes
{
    /// <summary>
    ///     Attribute to be used for a member of a storable class and that object will be ignored by siaqodb engine
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class IgnoreAttribute : Attribute
    {
    }
}