namespace sqoDB.MetaObjects
{
    internal class ArrayInfo
    {
        public int ElementTypeId; //used only if is jagged array
        public int NrElements;
        public byte[] rawArray;
    }
}