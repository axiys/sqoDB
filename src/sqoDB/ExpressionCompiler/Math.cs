//
// Math.cs
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

using System.Globalization;
using System.Linq.Expressions;

namespace System.Linq.jvm
{
    internal class Math
    {
        public static object Evaluate(object a, object b, Type t, ExpressionType et)
        {
            var tc = Type.GetTypeCode(t);
            if (tc == TypeCode.Object)
            {
                if (!t.IsNullable())
                    throw new NotImplementedException(
                        string.Format(
                            "Expression with Node type {0} for type {1}",
                            t.FullName,
                            tc));
                return EvaluateNullable(a, b, Type.GetTypeCode(t.GetGenericArguments()[0]), et);
            }

            return Evaluate(a, b, tc, et);
        }

        public static object EvaluateNullable(object a, object b, TypeCode tc, ExpressionType et)
        {
            object o = null;
            if (a == null || b == null)
            {
                if (tc != TypeCode.Boolean) return null;
                switch (et)
                {
                    case ExpressionType.And:
                        o = And(a, b);
                        break;
                    case ExpressionType.Or:
                        o = Or(a, b);
                        break;
                    case ExpressionType.ExclusiveOr:
                        o = ExclusiveOr(a, b);
                        break;
                }
            }
            else
            {
                o = Evaluate(a, b, tc, et);
            }

            return Convert2Nullable(o, tc);
        }

        private static object ExclusiveOr(object a, object b)
        {
            if (a == null || b == null) return null;
            return (bool)a ^ (bool)b;
        }

        public static object Or(object a, object b)
        {
            if (a == null)
            {
                if (b == null || !(bool)b) return null;
                return true;
            }

            if (b == null)
            {
                if (a == null || !(bool)a) return null;
                return true;
            }

            return (bool)a || (bool)b;
        }

        public static object And(object a, object b)
        {
            if (a == null)
            {
                if (b == null || (bool)b) return null;
                return false;
            }

            if (b == null)
            {
                if (a == null || (bool)a) return null;
                return false;
            }

            return (bool)a && (bool)b;
        }

        private static object Convert2Nullable(object o, TypeCode tc)
        {
            if (o == null) return null;
            switch (tc)
            {
                case TypeCode.Char:
                    return (char)o;
                case TypeCode.Byte:
                    return (byte)o;
                case TypeCode.Decimal:
                    return (decimal)o;
                case TypeCode.Double:
                    return (double)o;
                case TypeCode.Int16:
                    return (short)o;
                case TypeCode.Int32:
                    return (int)o;
                case TypeCode.Int64:
                    return (long)o;
                case TypeCode.UInt16:
                    return (ushort)o;
                case TypeCode.UInt32:
                    return (uint)o;
                case TypeCode.SByte:
                    return (sbyte)o;
                case TypeCode.Single:
                    return (float)o;
                case TypeCode.Boolean:
                    return (bool)o;
            }

            throw new NotImplementedException();
        }

        public static object Evaluate(object a, object b, TypeCode tc, ExpressionType et)
        {
            switch (tc)
            {
                case TypeCode.Boolean:
                    return Evaluate(Convert.ToBoolean(a), Convert.ToBoolean(b), et);
                case TypeCode.Char:
                    return Evaluate(Convert.ToChar(a), Convert.ToChar(b), et);
                case TypeCode.Byte:
                    return unchecked((byte)Evaluate(Convert.ToByte(a), Convert.ToByte(b), et));
                case TypeCode.Decimal:
                    return Evaluate(Convert.ToDecimal(a), Convert.ToDecimal(b), et);
                case TypeCode.Double:
                    return Evaluate(Convert.ToDouble(a), Convert.ToDouble(b), et);
                case TypeCode.Int16:
                    return unchecked((short)Evaluate(Convert.ToInt16(a), Convert.ToInt16(b), et));
                case TypeCode.Int32:
                    return Evaluate(Convert.ToInt32(a), Convert.ToInt32(b), et);
                case TypeCode.Int64:
                    return Evaluate(Convert.ToInt64(a), Convert.ToInt64(b), et);
                case TypeCode.UInt16:
                    return unchecked((ushort)Evaluate(Convert.ToUInt16(a), Convert.ToUInt16(b), et));
                case TypeCode.UInt32:
                    return Evaluate(Convert.ToUInt32(a), Convert.ToUInt32(b), et);
                case TypeCode.UInt64:
                    return Evaluate(Convert.ToUInt64(a), Convert.ToUInt64(b), et);
                case TypeCode.SByte:
                    return unchecked((sbyte)Evaluate(Convert.ToSByte(a), Convert.ToSByte(b), et));
                case TypeCode.Single:
                    return Evaluate(Convert.ToSingle(a), Convert.ToSingle(b), et);
            }

            throw new NotImplementedException();
        }

        public static object NegateChecked(object a, TypeCode tc)
        {
            switch (tc)
            {
                case TypeCode.Char:
                    return checked(-Convert.ToChar(a));
                case TypeCode.Byte:
                    return checked(-Convert.ToByte(a));
                case TypeCode.Decimal:
                    return -Convert.ToDecimal(a);
                case TypeCode.Double:
                    return -Convert.ToDouble(a);
                case TypeCode.Int16:
                    return checked(-Convert.ToInt16(a));
                case TypeCode.Int32:
                    return checked(-Convert.ToInt32(a));
                case TypeCode.Int64:
                    return checked(-Convert.ToInt64(a));
                case TypeCode.UInt16:
                    return checked(-Convert.ToUInt16(a));
                case TypeCode.UInt32:
                    return checked(-Convert.ToUInt32(a));
                case TypeCode.SByte:
                    return checked(-Convert.ToSByte(a));
                case TypeCode.Single:
                    return -Convert.ToSingle(a);
            }

            throw new NotImplementedException();
        }

        private static object CreateInstance(Type type, params object[] arguments)
        {
            return type.GetConstructor(
                (from argument in arguments select argument.GetType()).ToArray()).Invoke(arguments);
        }

        public static object ConvertToTypeChecked(object a, Type fromType, Type toType)
        {
            if (toType.IsNullable())
                return a == null
                    ? a
                    : CreateInstance(toType,
                        ConvertToTypeChecked(a, fromType.GetNotNullableType(), toType.GetNotNullableType()));

            if (a == null)
            {
                if (!toType.IsValueType)
                    return a;
                if (fromType.IsNullable())
                    throw new InvalidOperationException("Nullable object must have a value");
            }

            if (IsType(toType, a)) return a;

            if (fromType.IsPrimitiveConversion(toType))
                return Convert.ChangeType(a, toType, CultureInfo.CurrentCulture);

            throw new NotImplementedException(
                string.Format("No Convert defined for type {0} ", toType));
        }

        public static object ConvertToTypeUnchecked(object a, Type fromType, Type toType)
        {
            if (toType.IsNullable())
                return a == null
                    ? a
                    : CreateInstance(toType,
                        ConvertToTypeUnchecked(a, fromType.GetNotNullableType(), toType.GetNotNullableType()));

            if (a == null)
            {
                if (!toType.IsValueType)
                    return a;
                if (fromType.IsNullable())
                    throw new InvalidOperationException("Nullable object must have a value");
            }

            if (IsType(toType, a))
                return a;

            if (fromType.IsPrimitiveConversion(toType))
                return Conversion.ConvertPrimitiveUnChecked(fromType, toType, a);

            if (toType.IsEnum) return Enum.ToObject(toType, a);

            throw new NotImplementedException(
                string.Format("No Convert defined for type {0} ", toType));
        }

        public static bool IsType(Type t, object o)
        {
            return t.IsInstanceOfType(o);
        }

        public static object Negate(object a, TypeCode tc)
        {
            switch (tc)
            {
                case TypeCode.Char:
                    return unchecked(-Convert.ToChar(a));
                case TypeCode.Byte:
                    return unchecked(-Convert.ToByte(a));
                case TypeCode.Decimal:
                    return -Convert.ToDecimal(a);
                case TypeCode.Double:
                    return -Convert.ToDouble(a);
                case TypeCode.Int16:
                    return unchecked(-Convert.ToInt16(a));
                case TypeCode.Int32:
                    return unchecked(-Convert.ToInt32(a));
                case TypeCode.Int64:
                    return unchecked(-Convert.ToInt64(a));
                case TypeCode.UInt16:
                    return unchecked(-Convert.ToUInt16(a));
                case TypeCode.UInt32:
                    return unchecked(-Convert.ToUInt32(a));
                case TypeCode.SByte:
                    return unchecked(-Convert.ToSByte(a));
                case TypeCode.Single:
                    return -Convert.ToSingle(a);
            }

            throw new NotImplementedException();
        }

        public static object RightShift(object a, int n, TypeCode tc)
        {
            switch (tc)
            {
                case TypeCode.Int16:
                    return Convert.ToInt16(a) >> n;
                case TypeCode.Int32:
                    return Convert.ToInt32(a) >> n;
                case TypeCode.Int64:
                    return Convert.ToInt64(a) >> n;
                case TypeCode.UInt16:
                    return Convert.ToUInt16(a) >> n;
                case TypeCode.UInt32:
                    return Convert.ToUInt32(a) >> n;
                case TypeCode.UInt64:
                    return Convert.ToUInt64(a) >> n;
            }

            throw new NotImplementedException();
        }

        public static object LeftShift(object a, int n, TypeCode tc)
        {
            switch (tc)
            {
                case TypeCode.Int16:
                    return Convert.ToInt16(a) << n;
                case TypeCode.Int32:
                    return Convert.ToInt32(a) << n;
                case TypeCode.Int64:
                    return Convert.ToInt64(a) << n;
                case TypeCode.UInt16:
                    return Convert.ToUInt16(a) << n;
                case TypeCode.UInt32:
                    return Convert.ToUInt32(a) << n;
                case TypeCode.UInt64:
                    return Convert.ToUInt64(a) << n;
            }

            throw new NotImplementedException();
        }

        private static decimal Evaluate(decimal a, decimal b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return a + b;
                case ExpressionType.AddChecked:
                    return a + b;
                case ExpressionType.Subtract:
                    return a - b;
                case ExpressionType.SubtractChecked:
                    return a - b;
                case ExpressionType.Multiply:
                    return a * b;
                case ExpressionType.MultiplyChecked:
                    return a * b;
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
            }

            throw new NotImplementedException();
        }

        private static double Evaluate(double a, double b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return a + b;
                case ExpressionType.AddChecked:
                    return a + b;
                case ExpressionType.Subtract:
                    return a - b;
                case ExpressionType.SubtractChecked:
                    return a - b;
                case ExpressionType.Multiply:
                    return a * b;
                case ExpressionType.MultiplyChecked:
                    return a * b;
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
                case ExpressionType.Power:
                    return System.Math.Pow(a, b);
            }

            throw new NotImplementedException();
        }

        private static int Evaluate(short a, short b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return unchecked(a + b);
                case ExpressionType.AddChecked:
                    return checked(a + b);
                case ExpressionType.Subtract:
                    return unchecked(a - b);
                case ExpressionType.SubtractChecked:
                    return checked(a - b);
                case ExpressionType.Multiply:
                    return unchecked(a * b);
                case ExpressionType.MultiplyChecked:
                    return checked(a * b);
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }

        private static int Evaluate(int a, int b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return unchecked(a + b);
                case ExpressionType.AddChecked:
                    return checked(a + b);
                case ExpressionType.Subtract:
                    return unchecked(a - b);
                case ExpressionType.SubtractChecked:
                    return checked(a - b);
                case ExpressionType.Multiply:
                    return unchecked(a * b);
                case ExpressionType.MultiplyChecked:
                    return checked(a * b);
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }

        private static long Evaluate(long a, long b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return unchecked(a + b);
                case ExpressionType.AddChecked:
                    return checked(a + b);
                case ExpressionType.Subtract:
                    return unchecked(a - b);
                case ExpressionType.SubtractChecked:
                    return checked(a - b);
                case ExpressionType.Multiply:
                    return unchecked(a * b);
                case ExpressionType.MultiplyChecked:
                    return checked(a * b);
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }

        private static int Evaluate(ushort a, ushort b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return unchecked(a + b);
                case ExpressionType.AddChecked:
                    return checked(a + b);
                case ExpressionType.Subtract:
                    return unchecked(a - b);
                case ExpressionType.SubtractChecked:
                    return checked((ushort)(a - b));
                case ExpressionType.Multiply:
                    return unchecked(a * b);
                case ExpressionType.MultiplyChecked:
                    return checked(a * b);
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }

        private static uint Evaluate(uint a, uint b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return unchecked(a + b);
                case ExpressionType.AddChecked:
                    return checked(a + b);
                case ExpressionType.Subtract:
                    return unchecked(a - b);
                case ExpressionType.SubtractChecked:
                    return checked(a - b);
                case ExpressionType.Multiply:
                    return unchecked(a * b);
                case ExpressionType.MultiplyChecked:
                    return checked(a * b);
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }

        private static ulong Evaluate(ulong a, ulong b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return unchecked(a + b);
                case ExpressionType.AddChecked:
                    return checked(a + b);
                case ExpressionType.Subtract:
                    return unchecked(a - b);
                case ExpressionType.SubtractChecked:
                    return checked(a - b);
                case ExpressionType.Multiply:
                    return unchecked(a * b);
                case ExpressionType.MultiplyChecked:
                    return checked(a * b);
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }

        private static object Evaluate(char a, char b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }

        private static int Evaluate(sbyte a, sbyte b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return unchecked(a + b);
                case ExpressionType.AddChecked:
                    return checked(a + b);
                case ExpressionType.Subtract:
                    return unchecked(a - b);
                case ExpressionType.SubtractChecked:
                    return checked(a - b);
                case ExpressionType.Multiply:
                    return unchecked(a * b);
                case ExpressionType.MultiplyChecked:
                    return checked(a * b);
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }

        private static int Evaluate(byte a, byte b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return unchecked(a + b);
                case ExpressionType.AddChecked:
                    return checked(a + b);
                case ExpressionType.Subtract:
                    return unchecked(a - b);
                case ExpressionType.SubtractChecked:
                    return checked(a - b);
                case ExpressionType.Multiply:
                    return unchecked(a * b);
                case ExpressionType.MultiplyChecked:
                    return checked(a * b);
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }

        private static float Evaluate(float a, float b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.Add:
                    return a + b;
                case ExpressionType.AddChecked:
                    return a + b;
                case ExpressionType.Subtract:
                    return a - b;
                case ExpressionType.SubtractChecked:
                    return a - b;
                case ExpressionType.Multiply:
                    return a * b;
                case ExpressionType.MultiplyChecked:
                    return a * b;
                case ExpressionType.Divide:
                    return a / b;
                case ExpressionType.Modulo:
                    return a % b;
            }

            throw new NotImplementedException();
        }

        private static bool Evaluate(bool a, bool b, ExpressionType et)
        {
            switch (et)
            {
                case ExpressionType.ExclusiveOr:
                    return a ^ b;
                case ExpressionType.And:
                    return a & b;
                case ExpressionType.Or:
                    return a | b;
            }

            throw new NotImplementedException();
        }
    }
}