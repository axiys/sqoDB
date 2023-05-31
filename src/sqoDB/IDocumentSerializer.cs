using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sqoDB
{
    public interface IDocumentSerializer
    {
        object Deserialize(Type type, byte[] objectBytes);
        byte[] Serialize(object obj);

    }
}
