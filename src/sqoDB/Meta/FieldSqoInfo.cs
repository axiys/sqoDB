using System;
using System.Reflection;

namespace sqoDB.Meta
{
    internal class FieldSqoInfo
    {
        public FieldSqoInfo(int attTypeId, Type attType)
        {
            if (attType.IsEnum())
            {
                var enumType = Enum.GetUnderlyingType(attType);
                AttributeType = enumType;
            }
            else
            {
                AttributeType = attType;
            }

            AttributeTypeId = attTypeId;
        }

        public FieldSqoInfo(Type attType)
        {
            AttributeType = attType;
        }

        public FieldSqoInfo()
        {
        }

        public int AttributeTypeId { get; set; }

        public Type AttributeType { get; set; }

        public string Name { get; set; }

        public FieldInfo FInfo { get; set; }

        public AttributeHeader Header { get; } = new AttributeHeader();

        public bool IsText { get; set; }
    }
}