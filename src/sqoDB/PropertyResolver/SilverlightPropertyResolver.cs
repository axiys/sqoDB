using System;
using System.Reflection;
using sqoDB.Meta;
using sqoDB;
namespace sqoDB.PropertyResolver
{
    class SilverlightPropertyResolver
    {
        public static string GetPrivateFieldName(PropertyInfo pi, Type ti)
        {
            string backingField = "<" + pi.Name + ">";
            FieldInfo[] fields=ti.GetFields(BindingFlags.FlattenHierarchy | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static);
            foreach (FieldInfo fi in fields)
            {
                if (fi.Name.StartsWith(backingField))
                {
                    return fi.Name;
                }
            }
            
            return null;
           
        }
    }
}
