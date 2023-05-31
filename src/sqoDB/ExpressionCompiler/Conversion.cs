//
// Conversion.cs
//
// (C) 2008 Mainsoft, Inc. (http://www.mainsoft.com)
// (C) 2008 db4objects, Inc. (http://www.db4o.com)
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

namespace System.Linq.jvm
{
    internal class Conversion
    {
        public static object ConvertPrimitiveUnChecked(Type from, Type to, object value)
        {
            switch (Type.GetTypeCode(from))
            {
                case TypeCode.Byte:
                    return ConvertByte((byte)value, to);
                case TypeCode.Char:
                    return ConvertChar((char)value, to);
                case TypeCode.Decimal:
                    return ConvertDecimal((decimal)value, to);
                case TypeCode.Double:
                    return ConvertDouble((double)value, to);
                case TypeCode.Int16:
                    return ConvertShort((short)value, to);
                case TypeCode.Int32:
                    return ConvertInt((int)value, to);
                case TypeCode.Int64:
                    return ConvertLong((long)value, to);
                case TypeCode.SByte:
                    return ConvertSByte((sbyte)value, to);
                case TypeCode.Single:
                    return ConvertFloat((float)value, to);
                case TypeCode.UInt16:
                    return ConvertUShort((ushort)value, to);
                case TypeCode.UInt32:
                    return ConvertUInt((uint)value, to);
                case TypeCode.UInt64:
                    return ConvertULong((ulong)value, to);
                default:
                    throw new NotImplementedException();
            }
        }

        private static object ConvertByte(byte b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertChar(char b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertDecimal(decimal b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)(short)b;
                    case TypeCode.Decimal:
                        return b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertDouble(double b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertShort(short b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertInt(int b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertLong(long b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertSByte(sbyte b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertFloat(float b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertUShort(ushort b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertUInt(uint b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return b;
                    case TypeCode.UInt64:
                        return (ulong)b;
                }

                return null;
            }
        }

        private static object ConvertULong(ulong b, Type to)
        {
            unchecked
            {
                switch (Type.GetTypeCode(to))
                {
                    case TypeCode.Byte:
                        return (byte)b;
                    case TypeCode.Char:
                        return (char)b;
                    case TypeCode.Decimal:
                        return (decimal)b;
                    case TypeCode.Double:
                        return (double)b;
                    case TypeCode.Int16:
                        return (short)b;
                    case TypeCode.Int32:
                        return (int)b;
                    case TypeCode.Int64:
                        return (long)b;
                    case TypeCode.SByte:
                        return (sbyte)b;
                    case TypeCode.Single:
                        return (float)b;
                    case TypeCode.UInt16:
                        return (ushort)b;
                    case TypeCode.UInt32:
                        return (uint)b;
                    case TypeCode.UInt64:
                        return b;
                }

                return null;
            }
        }
    }
}