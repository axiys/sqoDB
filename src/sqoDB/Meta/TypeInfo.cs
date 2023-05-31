using System;
using System.Collections.Generic;
using System.Text;
using sqoDB.Indexes;
using sqoDB.MetaObjects;

namespace sqoDB.Meta
{
    internal class SqoTypeInfo
    {
        public List<FieldSqoInfo> Fields = new List<FieldSqoInfo>();

        public List<FieldSqoInfo> IndexedFields = new List<FieldSqoInfo>();

        public bool IsOld;
        private string typeName;
        public List<FieldSqoInfo> UniqueFields = new List<FieldSqoInfo>();

        public SqoTypeInfo(Type type)
        {
            if (type == typeof(RawdataInfo))
            {
                typeName = "sqoDB.MetaObjects.RawdataInfo";
                Type = type;
            }
            else if (type == typeof(IndexInfo2))
            {
                typeName = "sqoDB.Indexes.IndexInfo2";
                Type = type;
            }
            else if (type.IsGenericType() && type.GetGenericTypeDefinition() == typeof(BTreeNode<>))
            {
                typeName = type.Namespace + "." + type.Name;
                AddGenericsInfo(type, ref typeName);
                Type = type;
            }

            else
            {
                Type = type;
#if SILVERLIGHT
			string tName = type.AssemblyQualifiedName;
#else

                var tName = BuildTypeName(type);
#endif

                typeName = tName;
            }
        }


        public SqoTypeInfo()
        {
        }

        public string TypeName
        {
            get => typeName;
            set => typeName = value;
        }

        public Type Type { get; set; }

        public TypeHeader Header { get; } = new TypeHeader();

        public string FileNameForManager { get; set; }

        private string BuildTypeName(Type type)
        {
            var onlyTypeName = type.Namespace + "." + type.Name;
            AddGenericsInfo(type, ref onlyTypeName);

#if SILVERLIGHT
            string assemblyName = type.Assembly.FullName.Split(',')[0];
#elif WinRT
            string assemblyName = type.GetTypeInfo().Assembly.GetName().Name;
#else
            var assemblyName = type.Assembly.GetName().Name;
#endif

            string[] tNames = { onlyTypeName, assemblyName };

            return tNames[0] + ", " + tNames[1];
        }

        private void AddGenericsInfo(Type type, ref string onlyTypeName)
        {
            if (type.IsGenericType())
            {
                var gParams = type.GetGenericArguments();
                var builder = new StringBuilder(onlyTypeName);
                builder.Append("[");
                for (var i = 0; i < gParams.Length; ++i)
                {
                    if (i > 0) builder.Append(", ");
                    builder.Append("[");
                    builder.Append(BuildTypeName(gParams[i]));
                    builder.Append("]");
                }

                builder.Append("]");
                onlyTypeName = builder.ToString();
            }
        }
    }
}