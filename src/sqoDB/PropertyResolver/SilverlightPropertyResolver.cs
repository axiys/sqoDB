using System;
using System.Reflection;

namespace sqoDB.PropertyResolver
{
    internal class SilverlightPropertyResolver
    {
        public static string GetPrivateFieldName(PropertyInfo pi, Type ti)
        {
            var backingField = "<" + pi.Name + ">";
            var fields = ti.GetFields(BindingFlags.FlattenHierarchy | BindingFlags.NonPublic | BindingFlags.Instance |
                                      BindingFlags.Public | BindingFlags.Static);
            foreach (var fi in fields)
                if (fi.Name.StartsWith(backingField))
                    return fi.Name;

            return null;
        }
    }
}