using System;
using System.Collections;
using System.Threading.Tasks;
using sqoDB.Indexes;
using sqoDB.Meta;

namespace sqoDB.Core
{
    internal class FixedArrayByteTransformer : IByteTransformer
    {
        private readonly FieldSqoInfo fi;
        private readonly ObjectSerializer serializer;
        private readonly SqoTypeInfo ti;

        public FixedArrayByteTransformer(ObjectSerializer serializer, SqoTypeInfo ti, FieldSqoInfo fi)
        {
            this.serializer = serializer;
            this.ti = ti;
            this.fi = fi;
        }

        public byte[] GetBytes(object obj)
        {
            var elementType = fi.AttributeType.GetElementType();
            var elementTypeId = MetaExtractor.GetAttributeType(elementType);
            var elementSize = MetaExtractor.GetSizeOfField(elementTypeId);
            var rawLength = ((IList)obj).Count * elementSize;
            var rawArray = new byte[rawLength];
            //build array for elements
            var currentIndex = 0;
            foreach (var elem in (IList)obj)
            {
                byte[] elemArray = null;
                if (elementTypeId == MetaExtractor.complexID)
                {
                    elemArray = serializer.GetComplexObjectBytes(elem, true);
                }
                else
                {
                    var elemObj = elem;
                    if (elem == null)
                        if (elementType == typeof(string))
                            elemObj = string.Empty;
                    elemArray = ByteConverter.SerializeValueType(elemObj, elemObj.GetType(), elementSize,
                        MetaExtractor.GetAbsoluteSizeOfField(elementTypeId), ti.Header.version);
                }

                Array.Copy(elemArray, 0, rawArray, currentIndex, elemArray.Length);

                currentIndex += elemArray.Length;
            }

            return rawArray;
        }

        public object GetObject(byte[] arrayData)
        {
            var isArray = fi.AttributeType.IsArray;
            var elementType = fi.AttributeType.GetElementType();

            var elementTypeId = MetaExtractor.GetAttributeType(elementType);
            var elementSize = MetaExtractor.GetSizeOfField(elementTypeId);
            var nrElem = 0;
            if (fi.Name == "Keys" || fi.Name == "Values")
                nrElem = BTreeNode<int>.KEYS_PER_NODE;
            else //_childrenOIDs
                nrElem = BTreeNode<int>.CHILDREN_PER_NODE;

            Array ar = null;

            if (isArray) ar = Array.CreateInstance(elementType, nrElem);

            var currentIndex = 0;
            var elemBytes = new byte[elementSize];
            for (var i = 0; i < nrElem; i++)
            {
                Array.Copy(arrayData, currentIndex, elemBytes, 0, elementSize);
                currentIndex += elementSize;
                object obj = null;
                if (elementTypeId == MetaExtractor.complexID)
                    obj = serializer.ReadComplexObject(elemBytes, ti.Type, fi.Name);
                else
                    obj = ByteConverter.DeserializeValueType(elementType, elemBytes, true, ti.Header.version);
                if (isArray) ar.SetValue(obj, i);
            }

            return ar;
        }

#if ASYNC
        public async Task<byte[]> GetBytesAsync(object obj)
        {
            var elementType = fi.AttributeType.GetElementType();
            var elementTypeId = MetaExtractor.GetAttributeType(elementType);
            var elementSize = MetaExtractor.GetSizeOfField(elementTypeId);
            var rawLength = ((IList)obj).Count * elementSize;
            var rawArray = new byte[rawLength];
            //build array for elements
            var currentIndex = 0;
            foreach (var elem in (IList)obj)
            {
                byte[] elemArray = null;
                if (elementTypeId == MetaExtractor.complexID)
                {
                    elemArray = await serializer.GetComplexObjectBytesAsync(elem, true).ConfigureAwait(false);
                }
                else
                {
                    var elemObj = elem;
                    if (elem == null)
                        if (elementType == typeof(string))
                            elemObj = string.Empty;
                    elemArray = ByteConverter.SerializeValueType(elemObj, elemObj.GetType(), elementSize,
                        MetaExtractor.GetAbsoluteSizeOfField(elementTypeId), ti.Header.version);
                }

                Array.Copy(elemArray, 0, rawArray, currentIndex, elemArray.Length);

                currentIndex += elemArray.Length;
            }

            return rawArray;
        }

        public async Task<object> GetObjectAsync(byte[] arrayData)
        {
            var isArray = fi.AttributeType.IsArray;
            var elementType = fi.AttributeType.GetElementType();

            var elementTypeId = MetaExtractor.GetAttributeType(elementType);
            var elementSize = MetaExtractor.GetSizeOfField(elementTypeId);
            var nrElem = 0;
            if (fi.Name == "Keys" || fi.Name == "Values")
                nrElem = BTreeNode<int>.KEYS_PER_NODE;
            else //_childrenOIDs
                nrElem = BTreeNode<int>.CHILDREN_PER_NODE;

            Array ar = null;

            if (isArray) ar = Array.CreateInstance(elementType, nrElem);

            var currentIndex = 0;
            var elemBytes = new byte[elementSize];
            for (var i = 0; i < nrElem; i++)
            {
                Array.Copy(arrayData, currentIndex, elemBytes, 0, elementSize);
                currentIndex += elementSize;
                object obj = null;
                if (elementTypeId == MetaExtractor.complexID)
                    obj = await serializer.ReadComplexObjectAsync(elemBytes, ti.Type, fi.Name).ConfigureAwait(false);
                else
                    obj = ByteConverter.DeserializeValueType(elementType, elemBytes, true, ti.Header.version);
                if (isArray) ar.SetValue(obj, i);
            }

            return ar;
        }
#endif
    }
}