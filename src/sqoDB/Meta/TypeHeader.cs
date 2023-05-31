using System;

namespace sqoDB.Meta
{
    internal class TypeHeader
    {
        public int headerSize;
        public DateTime lastUpdated;
        public int lengthOfRecord;
        public int NrFields;
        public int numberOfRecords;
        public int positionFirstRecord;
        public int TID;
        public int typeNameSize;
        public int Unused1;
        public int Unused2;
        public int Unused3;
        public int version = -35; //version 3.5
    }

    internal class AttributeHeader
    {
        public int SizeOfName { get; set; }

        public int Length { get; set; }

        public int PositionInRecord { get; set; }

        public int RealLength { get; set; }
    }
}