using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sqoDB.Attributes
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class IndexAttribute : System.Attribute
    {
        public IndexAttribute()
        {

        }

    }
}
