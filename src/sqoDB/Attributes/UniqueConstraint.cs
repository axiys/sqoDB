using System;

namespace sqoDB.Attributes
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class UniqueConstraint : Attribute
    {
    }
}