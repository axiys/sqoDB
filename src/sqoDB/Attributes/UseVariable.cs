using System;

namespace sqoDB.Attributes
{
    /// <summary>
    ///     Use this attribute if you use a Property and inside that
    ///     property use some complex code and when Siaqodb engine is not able
    ///     to get what is backing field of that Property, variableName is used for Siaqodb engine when that property is used
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class UseVariableAttribute : Attribute
    {
        internal string variableName;

        public UseVariableAttribute(string variableName)
        {
            this.variableName = variableName;
        }
    }
}