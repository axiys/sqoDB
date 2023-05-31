using System.Reflection;
using sqoDB.Attributes;

namespace sqoDB.Indexes
{
    [Obfuscation(Exclude = true)]
    internal class IndexInfo2
    {
        public int OID { get; set; }
        public int RootOID { get; set; }
        [Text] public string IndexName { get; set; }
#if SILVERLIGHT
        public object GetValue(System.Reflection.FieldInfo field)
        {
            return field.GetValue(this);
        }
        public void SetValue(System.Reflection.FieldInfo field, object value)
        {

            field.SetValue(this, value);

        }
#endif
    }
}