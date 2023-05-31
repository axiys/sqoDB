using System.Collections;
using sqoDB.Indexes;
using sqoDB.Meta;

namespace sqoDB.Core
{
    internal class ByteTransformerFactory
    {
        public static IByteTransformer GetByteTransformer(ObjectSerializer serializer, RawdataSerializer rawSerializer,
            FieldSqoInfo fi, SqoTypeInfo ti, int parentOID)
        {
            if (fi.AttributeTypeId == MetaExtractor.complexID || fi.AttributeTypeId == MetaExtractor.documentID)
                return new ComplexTypeTransformer(serializer, ti, fi);

            if (typeof(IList).IsAssignableFrom(fi.AttributeType) || fi.IsText) //array
            {
                if (ti.Type != null) //is null when loaded by SiaqodbManager
                {
                    if (ti.Type.IsGenericType())
                    {
                        if (ti.Type.GetGenericTypeDefinition() == typeof(BTreeNode<>) &&
                            (fi.Name == "Keys" || fi.Name == "_childrenOIDs"))
                            return new FixedArrayByteTransformer(serializer, ti, fi);
                        return new ArrayByteTranformer(serializer, rawSerializer, ti, fi, parentOID);
                    }

                    return new ArrayByteTranformer(serializer, rawSerializer, ti, fi, parentOID);
                }

                return new ArrayByteTranformer(serializer, rawSerializer, ti, fi, parentOID);
            }

            if (fi.AttributeTypeId == MetaExtractor.dictionaryID)
                return new DictionaryByteTransformer(serializer, rawSerializer, ti, fi, parentOID);
            return new PrimitiveByteTransformer(fi, ti);
        }

        public static IByteTransformer GetByteTransformer(ObjectSerializer serializer, RawdataSerializer rawSerializer,
            FieldSqoInfo fi, SqoTypeInfo ti)
        {
            return GetByteTransformer(serializer, rawSerializer, fi, ti, -1);
        }
    }
}