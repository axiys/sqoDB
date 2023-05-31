using System;

namespace sqoDB
{
    public class SavingEventsArgs : EventArgs
    {
        internal SavingEventsArgs(Type t, object o)
        {
            ObjectType = t;
            Object = o;
        }

        public bool Cancel { get; set; }
        public Type ObjectType { get; }

        public object Object { get; }
    }

    public class SavedEventsArgs : EventArgs
    {
        internal SavedEventsArgs(Type t, object o)
        {
            ObjectType = t;
            Object = o;
        }

        public Type ObjectType { get; }

        public object Object { get; }

        public bool Inserted { get; internal set; }
    }

    public class DeletingEventsArgs : EventArgs
    {
        internal DeletingEventsArgs(Type t, int oid)
        {
            ObjectType = t;
            Object = oid;
        }

        public bool Cancel { get; set; }
        public Type ObjectType { get; }

        public int Object { get; }
    }

    public class DeletedEventsArgs : EventArgs
    {
        internal DeletedEventsArgs(Type t, int oid)
        {
            ObjectType = t;
            OID = oid;
        }

        public Type ObjectType { get; }

        public int OID { get; }
    }

    public class LoadingObjectEventArgs : EventArgs
    {
        internal LoadingObjectEventArgs(int oid, Type objectType)
        {
            OID = oid;
            ObjectType = objectType;
        }

        public bool Cancel { get; set; }
        public Type ObjectType { get; }

        public int OID { get; }

        public object Replace { get; set; }
    }

    public class LoadedObjectEventArgs : EventArgs
    {
        internal LoadedObjectEventArgs(int oid, object obj)
        {
            OID = oid;
            Object = obj;
        }

        public object Object { get; }

        public int OID { get; }
    }

    public class IndexesSaveAsyncFinishedArgs : EventArgs
    {
        public Exception Error { get; set; }
        public bool Succeeded { get; set; }
    }
}