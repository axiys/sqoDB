﻿namespace sqoDB
{
    public interface ISqoDataObject
    {
        int OID { get; set; }
#if SILVERLIGHT
        object GetValue(System.Reflection.FieldInfo field);
        void SetValue(System.Reflection.FieldInfo field, object value);

#endif
    }
}