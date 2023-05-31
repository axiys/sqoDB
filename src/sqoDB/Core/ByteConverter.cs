using System;
using System.Text;
using sqoDB.Exceptions;
using sqoDB.Utilities;

namespace sqoDB.Core
{
    internal class ByteConverter
    {
        public static byte[] BooleanToByteArray(bool b)
        {
            var ab = new byte[1];
            if (b)
            {
                ab[0] = 1;
                return ab;
            }

            // default is 0
            return ab;
        }

        public static bool ByteArrayToBoolean(byte[] bytes)
        {
            if (bytes[0] == 0)
            {
                bytes = null;
                return false;
            }

            bytes = null;
            return true;
        }

        public static byte[] ShortToByteArray(short s)
        {
            return BitConverter.GetBytes(s);
        }

        public static short ByteArrayToShort(byte[] bytes)
        {
            return BitConverter.ToInt16(bytes, 0);
        }


        public static byte[] CharToByteArray(char c)
        {
            return BitConverter.GetBytes(c);
        }

        public static char ByteArrayToChar(byte[] bytes)
        {
            return BitConverter.ToChar(bytes, 0);
        }

        public static byte[] IntToByteArray(int l)
        {
            return BitConverter.GetBytes(l);
        }

        public static int ByteArrayToInt(byte[] bytes)
        {
            return BitConverter.ToInt32(bytes, 0);
        }


        public static byte[] LongToByteArray(long l)
        {
            return BitConverter.GetBytes(l);
        }

        public static long ByteArrayToLong(byte[] bytes)
        {
            return BitConverter.ToInt64(bytes, 0);
        }

        public static byte[] DateToByteArray(DateTime date)
        {
            return LongToByteArray(date.Ticks);
        }

        public static DateTime ByteArrayToDate(byte[] bytes)
        {
            return new DateTime(ByteArrayToLong(bytes));
        }

        public static byte[] FloatToByteArray(float f)
        {
            return BitConverter.GetBytes(f);
        }

        public static float ByteArrayToFloat(byte[] bytes)
        {
            return BitConverter.ToSingle(bytes, 0);
        }


        public static byte[] DoubleToByteArray(double d)
        {
            return BitConverter.GetBytes(d);
        }

        public static double ByteArrayToDouble(byte[] bytes)
        {
            return BitConverter.ToDouble(bytes, 0);
        }

        internal static byte[] GetBytes(object obj, Type objectType)
        {
            if (objectType == typeof(int)) return BitConverter.GetBytes((int)obj);
            if (objectType == typeof(bool)) return new[] { (bool)obj ? (byte)1 : (byte)0 };
            if (objectType == typeof(byte)) return new[] { (byte)obj };
            if (objectType == typeof(sbyte)) return new[] { (byte)(sbyte)obj };
            if (objectType == typeof(short)) return BitConverter.GetBytes((short)obj);
            if (objectType == typeof(ushort)) return BitConverter.GetBytes((ushort)obj);
            if (objectType == typeof(uint)) return BitConverter.GetBytes((uint)obj);
            if (objectType == typeof(long)) return BitConverter.GetBytes((long)obj);
            if (objectType == typeof(ulong)) return BitConverter.GetBytes((ulong)obj);
            if (objectType == typeof(float)) return BitConverter.GetBytes((float)obj);
            if (objectType == typeof(double)) return BitConverter.GetBytes((double)obj);
            //if (objectType == typeof(decimal)) return BitConverter.GetBytes((double)obj);
            if (objectType == typeof(char)) return BitConverter.GetBytes((char)obj);

            if (objectType == typeof(IntPtr)) throw new NotSupportedException("IntPtr type is not supported.");
            if (objectType == typeof(UIntPtr)) throw new NotSupportedException("UIntPtr type is not supported.");

            throw new NotSupportedException(
                "Could not retrieve bytes from the object type " + objectType.FullName + ".");
        }

        public static byte[] Encrypt(byte[] by, int length)
        {
            if (SiaqodbConfigurator.EncryptedDatabase)
            {
                if (by.Length == length) // length%8==0 is the same
                {
                    SiaqodbConfigurator.Encryptor.Encrypt(by, 0, by.Length);
                    return by;
                }

                //padding
                var b = new byte[length];
                Array.Copy(by, 0, b, 0, by.Length);
                SiaqodbConfigurator.Encryptor.Encrypt(b, 0, b.Length);
                return b;
            }

            return by;
        }

        public static byte[] Decrypt(Type objectType, byte[] bytes)
        {
            if (SiaqodbConfigurator.EncryptedDatabase)
            {
                var lengthOfType = -1;
                if (objectType == typeof(string) || objectType == typeof(byte[]))
                    lengthOfType = bytes.Length;
                else
                    lengthOfType = MetaHelper.GetLengthOfType(objectType);

                SiaqodbConfigurator.Encryptor.Decrypt(bytes, 0, bytes.Length);

                if (bytes.Length == lengthOfType) return bytes;

                var realBytes = new byte[lengthOfType];
                Array.Copy(bytes, 0, realBytes, 0, lengthOfType); //un-padd the bytes

                return realBytes;
            }

            return bytes;
        }

        public static byte[] SerializeValueType(object obj, Type objectType, int length, int realLength, int dbVersion)
        {
            if (objectType.IsGenericType())
            {
                // Nullable type?
                var genericTypeDef = objectType.GetGenericTypeDefinition();
                if (genericTypeDef == typeof(Nullable<>))
                {
                    objectType = objectType.GetGenericArguments()[0];
                    var b = new byte[length];
                    if (obj == null)
                    {
                        b[0] = 1; // is null
                    }
                    else
                    {
                        b[0] = 0; // is not null
                        var serVal = SerializeValueType(obj, objectType, length - 1, realLength, dbVersion);
                        Array.Copy(serVal, 0, b, 1, length - 1);
                    }

                    return b;
                }

                throw new NotSupportedTypeException("Other than Nullable<> generic types is not supported");
            }

            if (objectType == typeof(string))
            {
                if (SiaqodbConfigurator.EncryptedDatabase)
                {
                    var b = new byte[realLength];
                    var strOnly = Encoding.UTF8.GetBytes((string)obj);
                    var currentLength = realLength > strOnly.Length ? strOnly.Length : realLength;
                    Array.Copy(strOnly, 0, b, 0, currentLength);
                    //added for Encryption support
                    return Encrypt(b, length);
                }
                else
                {
                    var b = new byte[length];
                    var strOnly = Encoding.UTF8.GetBytes((string)obj);
                    var currentLength = length > strOnly.Length ? strOnly.Length : length;
                    Array.Copy(strOnly, 0, b, 0, currentLength);
                    //added for Encryption support
                    return Encrypt(b, length);
                }
            }

            if (objectType == typeof(byte[]))
            {
                var objBytes = (byte[])obj;
                var b = new byte[length];
                Array.Copy(objBytes, 0, b, 0, objBytes.Length);
                return Encrypt(b, length);
            }
            else
            {
                var b = SerializeValueType(obj, objectType, dbVersion);

                return Encrypt(b, length);
            }
        }

        public static byte[] SerializeValueType(object obj, Type objectType, int dbVersion)
        {
            if (objectType.IsPrimitive()) return GetBytes(obj, objectType);

            if (objectType == typeof(DateTime))
            {
                if (dbVersion <= -25)
                {
                    var dt = (DateTime)obj;
                    if (SiaqodbConfigurator.DateTimeKindToSerialize != null &&
                        dt.Kind != SiaqodbConfigurator.DateTimeKindToSerialize)
                        dt = DateTime.SpecifyKind(dt, SiaqodbConfigurator.DateTimeKindToSerialize.Value);

                    return GetBytes(dt.Ticks, typeof(long));
                }
#if SILVERLIGHT || CF
                    DateTime dt = (DateTime)obj;
                    if (dt.Year < 1601) throw new SiaqodbException("DateTime values must be bigger then 1 Jan 1601");
                    return GetBytes(dt.ToFileTime(), typeof(long));

#else

                return GetBytes(((DateTime)obj).ToBinary(), typeof(long));
#endif
            }
#if !CF

            if (objectType == typeof(DateTimeOffset))
            {
                var dt = (DateTimeOffset)obj;
                var ticks = GetBytes(dt.Ticks, typeof(long));
                var offsetTicks = GetBytes(dt.Offset.Ticks, typeof(long));
                var allbytes = new byte[ticks.Length + offsetTicks.Length];
                Array.Copy(ticks, 0, allbytes, 0, ticks.Length);
                Array.Copy(offsetTicks, 0, allbytes, ticks.Length, offsetTicks.Length);
                return allbytes;
            }
#endif

            if (objectType == typeof(TimeSpan)) return GetBytes(((TimeSpan)obj).Ticks, typeof(long));

            if (objectType == typeof(Guid)) return ((Guid)obj).ToByteArray();

            if (objectType == typeof(string)) return Encoding.UTF8.GetBytes((string)obj);

            if (objectType.IsEnum())
            {
                var enumType = Enum.GetUnderlyingType(objectType);

                var realObject = Convertor.ChangeType(obj, enumType);

                return SerializeValueType(realObject, enumType, dbVersion);
            }

            if (objectType == typeof(decimal))
            {
                var bits = decimal.GetBits((decimal)obj);
                var bytes1 = IntToByteArray(bits[0]);
                var bytes2 = IntToByteArray(bits[1]);
                var bytes3 = IntToByteArray(bits[2]);
                var bytes4 = IntToByteArray(bits[3]);
                var all = new byte[16];
                Array.Copy(bytes1, 0, all, 0, 4);
                Array.Copy(bytes2, 0, all, 4, 4);
                Array.Copy(bytes3, 0, all, 8, 4);
                Array.Copy(bytes4, 0, all, 12, 4);
                return all;
            }

            throw new NotSupportedTypeException("Type: " + objectType + " not supported");
        }

        public static object DeserializeValueType(Type objectType, byte[] bytes, bool checkEncrypted, int dbVersion)
        {
            if (checkEncrypted)
            {
                if (objectType
                    .IsGenericType()) //nullable only here because on  DeserializeValueType(Type objectType,byte[] bytes) is used only for Metadata directly and in Metadata is not used Nullable fields
                {
                    // Nullable type?
                    var genericTypeDef = objectType.GetGenericTypeDefinition();
                    if (genericTypeDef == typeof(Nullable<>))
                    {
                        objectType = objectType.GetGenericArguments()[0];
                        if (bytes[0] == 1) return null;

                        var realBytes = new byte[bytes.Length - 1];
                        Array.Copy(bytes, 1, realBytes, 0, bytes.Length - 1);
                        return DeserializeValueType(objectType, realBytes, true, dbVersion);
                    }

                    throw new NotSupportedTypeException("Type: " + objectType + " not supported");
                }

                bytes = Decrypt(objectType, bytes);

                return DeserializeValueType(objectType, bytes, dbVersion);
            }

            return DeserializeValueType(objectType, bytes, dbVersion);
        }

        public static object DeserializeValueType(Type objectType, byte[] bytes, int dbVersion)
        {
            if (objectType.IsPrimitive()) return ReadBytes(bytes, objectType);

            if (objectType == typeof(DateTime))
            {
                var readLong = ByteArrayToLong(bytes);
                if (dbVersion <= -25)
                {
                    var dt = new DateTime(readLong);
                    if (SiaqodbConfigurator.DateTimeKindToSerialize != null)
                        return DateTime.SpecifyKind(dt, SiaqodbConfigurator.DateTimeKindToSerialize.Value);
                    return dt;
                }
#if SILVERLIGHT || CF
                    return DateTime.FromFileTime(readLong);

#else

                return DateTime.FromBinary(readLong);
#endif
            }
#if !CF

            if (objectType == typeof(DateTimeOffset))
            {
                var ticks = new byte[8];
                var offsetTicks = new byte[8];
                Array.Copy(bytes, 0, ticks, 0, 8);
                Array.Copy(bytes, 8, offsetTicks, 0, 8);
                return new DateTimeOffset(new DateTime(ByteArrayToLong(ticks)),
                    new TimeSpan(ByteArrayToLong(offsetTicks)));
            }
#endif

            if (objectType == typeof(TimeSpan))
            {
                var readLong = ByteArrayToLong(bytes);
                return TimeSpan.FromTicks(readLong);
            }

            if (objectType == typeof(string))
            {
#if SILVERLIGHT || CF || WinRT
				string s = Encoding.UTF8.GetString(bytes, 0, bytes.Length);
				return s.TrimEnd('\0');

#else

                var s = Encoding.UTF8.GetString(bytes);
                return s.TrimEnd('\0');

#endif
            }

            if (objectType == typeof(decimal))
            {
                var bits = new int[4];
                var bytes1 = new byte[4];
                var bytes2 = new byte[4];
                var bytes3 = new byte[4];
                var bytes4 = new byte[4];

                Array.Copy(bytes, 0, bytes1, 0, 4);
                Array.Copy(bytes, 4, bytes2, 0, 4);
                Array.Copy(bytes, 8, bytes3, 0, 4);
                Array.Copy(bytes, 12, bytes4, 0, 4);


                bits[0] = ByteArrayToInt(bytes1);
                bits[1] = ByteArrayToInt(bytes2);
                bits[2] = ByteArrayToInt(bytes3);
                bits[3] = ByteArrayToInt(bytes4);
                return new decimal(bits);
            }

            if (objectType == typeof(Guid)) return new Guid(bytes);

            if (objectType.IsEnum())
            {
                var enumType = Enum.GetUnderlyingType(objectType);
                var realObject = DeserializeValueType(enumType, bytes, dbVersion);
                return Enum.ToObject(objectType, realObject);
            }

            if (objectType == typeof(byte[]))
                return bytes;
            throw new NotSupportedTypeException("Type: " + objectType + " not supported");
        }

        internal static object ReadBytes(byte[] bytes, Type objectType)
        {
            if (objectType == typeof(bool)) return bytes[0] == 1 ? true : false;
            if (objectType == typeof(byte)) return bytes[0];
            if (objectType == typeof(sbyte)) return (sbyte)bytes[0];
            if (objectType == typeof(short)) return BitConverter.ToInt16(bytes, 0);
            if (objectType == typeof(ushort)) return BitConverter.ToUInt16(bytes, 0);
            if (objectType == typeof(int)) return BitConverter.ToInt32(bytes, 0);
            if (objectType == typeof(uint)) return BitConverter.ToUInt32(bytes, 0);
            if (objectType == typeof(long)) return BitConverter.ToInt64(bytes, 0);
            if (objectType == typeof(ulong)) return BitConverter.ToUInt64(bytes, 0);
            if (objectType == typeof(float)) return BitConverter.ToSingle(bytes, 0);
            if (objectType == typeof(double)) return BitConverter.ToDouble(bytes, 0);
            if (objectType == typeof(char)) return BitConverter.ToChar(bytes, 0);

            if (objectType == typeof(IntPtr)) throw new NotSupportedTypeException("Type: IntPtr not supported");

            throw new NotSupportedTypeException("Could not retrieve bytes from the object type " + objectType.FullName +
                                                ".");
        }
    }
}