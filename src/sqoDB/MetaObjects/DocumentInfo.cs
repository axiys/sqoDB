using System.Reflection;
using sqoDB.Attributes;

namespace sqoDB.MetaObjects
{
    [Obfuscation(Exclude = true)]
    internal class DocumentInfo
    {
        public int OID { get; set; }
        [MaxLength(300)] public string TypeName;
        public byte[] Document;


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