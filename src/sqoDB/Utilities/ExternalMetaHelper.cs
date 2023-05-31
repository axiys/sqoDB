using System.Reflection;
using sqoDB.Exceptions;
using sqoDB.PropertyResolver;

namespace sqoDB.Utilities
{
    public static class ExternalMetaHelper
    {
        public static string GetBackingField(MemberInfo mi)
        {
            var pi = mi as PropertyInfo;
#if SILVERLIGHT || CF || UNITY3D || WinRT || MONODROID
            string fieldName = SilverlightPropertyResolver.GetPrivateFieldName(pi, pi.DeclaringType);
             if (fieldName != null)
            {
                return fieldName;
            }
#else
            var fInfo = BackingFieldResolver.GetBackingField(pi);

            if (fInfo != null)
                return fInfo.Name;
#endif

            var fld = MetaHelper.GetBackingFieldByAttribute(mi);
            if (fld != null)
                return fld;
            throw new SiaqodbException("A Property must have UseVariable Attribute set");
        }
    }
}