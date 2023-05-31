using System;

namespace sqoDB
{
    public interface IDocumentSerializer
    {
        object Deserialize(Type type, byte[] objectBytes);
        byte[] Serialize(object obj);
    }
}