using System.Reflection;

namespace sqoDB.MetaObjects
{
    [Obfuscation(Exclude = true)]
    internal class RawdataInfo
    {
        public int OID { get; set; }

        public int Length;
        public int ElementLength;
        public long Position;

        public bool IsFree;

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