//
// MethodBaseRocks.cs
//
// Author:
//   Jb Evain (jbevain@novell.com)
//
// (C) 2009 Novell, Inc. (http://www.novell.com)
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

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace Mono.Reflection
{
    internal sealed class Instruction
    {
        private OpCode opcode;

        internal Instruction(int offset, OpCode opcode)
        {
            Offset = offset;
            this.opcode = opcode;
        }

        public int Offset { get; }

        public OpCode OpCode => opcode;

        public object Operand { get; internal set; }

        public Instruction Previous { get; internal set; }

        public Instruction Next { get; internal set; }

        public int GetSize()
        {
            var size = opcode.Size;

            switch (opcode.OperandType)
            {
                case OperandType.InlineSwitch:
                    size += (1 + ((int[])Operand).Length) * 4;
                    break;
                case OperandType.InlineI8:
                case OperandType.InlineR:
                    size += 8;
                    break;
                case OperandType.InlineBrTarget:
                case OperandType.InlineField:
                case OperandType.InlineI:
                case OperandType.InlineMethod:
                case OperandType.InlineString:
                case OperandType.InlineTok:
                case OperandType.InlineType:
                case OperandType.ShortInlineR:
                    size += 4;
                    break;
                case OperandType.InlineVar:
                    size += 2;
                    break;
                case OperandType.ShortInlineBrTarget:
                case OperandType.ShortInlineI:
                case OperandType.ShortInlineVar:
                    size += 1;
                    break;
            }

            return size;
        }

        public override string ToString()
        {
            return opcode.Name;
        }
    }

    internal class MethodBodyReader
    {
        private static readonly OpCode[] one_byte_opcodes;
        private static readonly OpCode[] two_bytes_opcodes;
        private readonly MethodBody body;
        private readonly ByteBuffer il;
        private readonly List<Instruction> instructions = new List<Instruction>();
        private readonly IList<LocalVariableInfo> locals;

        private readonly MethodBase method;
        private readonly Type[] method_arguments;
        private readonly Module module;
        private readonly ParameterInfo[] parameters;
        private readonly Type[] type_arguments;

        static MethodBodyReader()
        {
            one_byte_opcodes = new OpCode[0xe1];
            two_bytes_opcodes = new OpCode[0x1f];

            var fields = GetOpCodeFields();

            for (var i = 0; i < fields.Length; i++)
            {
                var opcode = (OpCode)fields[i].GetValue(null);
                if (opcode.OpCodeType == OpCodeType.Nternal)
                    continue;

                if (opcode.Size == 1)
                    one_byte_opcodes[opcode.Value] = opcode;
                else
                    two_bytes_opcodes[opcode.Value & 0xff] = opcode;
            }
        }

        private MethodBodyReader(MethodBase method)
        {
            this.method = method;

            body = method.GetMethodBody();
            if (body == null)
                throw new ArgumentException();

            var bytes = body.GetILAsByteArray();
            if (bytes == null)
                throw new ArgumentException();

            if (!(method is ConstructorInfo))
                method_arguments = method.GetGenericArguments();

            if (method.DeclaringType != null)
                type_arguments = method.DeclaringType.GetGenericArguments();

            parameters = method.GetParameters();
            locals = body.LocalVariables;
            module = method.Module;
            il = new ByteBuffer(bytes);
        }

        private static FieldInfo[] GetOpCodeFields()
        {
            return typeof(OpCodes).GetFields(BindingFlags.Public | BindingFlags.Static);
        }

        private void ReadInstructions()
        {
            Instruction previous = null;

            while (il.position < il.buffer.Length)
            {
                var instruction = new Instruction(il.position, ReadOpCode());

                ReadOperand(instruction);

                if (previous != null)
                {
                    instruction.Previous = previous;
                    previous.Next = instruction;
                }

                instructions.Add(instruction);
                previous = instruction;
            }
        }

        private void ReadOperand(Instruction instruction)
        {
            switch (instruction.OpCode.OperandType)
            {
                case OperandType.InlineNone:
                    break;
                case OperandType.InlineSwitch:
                    var length = il.ReadInt32();
                    var branches = new int[length];
                    var offsets = new int[length];
                    for (var i = 0; i < length; i++)
                        offsets[i] = il.ReadInt32();
                    for (var i = 0; i < length; i++)
                        branches[i] = il.position + offsets[i];

                    instruction.Operand = branches;
                    break;
                case OperandType.ShortInlineBrTarget:
                    instruction.Operand = (sbyte)(il.ReadByte() + il.position);
                    break;
                case OperandType.InlineBrTarget:
                    instruction.Operand = il.ReadInt32() + il.position;
                    break;
                case OperandType.ShortInlineI:
                    if (instruction.OpCode == OpCodes.Ldc_I4_S)
                        instruction.Operand = (sbyte)il.ReadByte();
                    else
                        instruction.Operand = il.ReadByte();
                    break;
                case OperandType.InlineI:
                    instruction.Operand = il.ReadInt32();
                    break;
                case OperandType.ShortInlineR:
                    instruction.Operand = il.ReadSingle();
                    break;
                case OperandType.InlineR:
                    instruction.Operand = il.ReadDouble();
                    break;
                case OperandType.InlineI8:
                    instruction.Operand = il.ReadInt64();
                    break;
                case OperandType.InlineSig:
                    instruction.Operand = module.ResolveSignature(il.ReadInt32());
                    break;
                case OperandType.InlineString:
                    instruction.Operand = module.ResolveString(il.ReadInt32());
                    break;
                case OperandType.InlineTok:
                    instruction.Operand = module.ResolveMember(il.ReadInt32(), type_arguments, method_arguments);
                    break;
                case OperandType.InlineType:
                    instruction.Operand = module.ResolveType(il.ReadInt32(), type_arguments, method_arguments);
                    break;
                case OperandType.InlineMethod:
                    instruction.Operand = module.ResolveMethod(il.ReadInt32(), type_arguments, method_arguments);
                    break;
                case OperandType.InlineField:
                    instruction.Operand = module.ResolveField(il.ReadInt32(), type_arguments, method_arguments);
                    break;
                case OperandType.ShortInlineVar:
                    instruction.Operand = GetVariable(instruction, il.ReadByte());
                    break;
                case OperandType.InlineVar:
                    instruction.Operand = GetVariable(instruction, il.ReadInt16());
                    break;
                default:
                    throw new NotSupportedException();
            }
        }

        private object GetVariable(Instruction instruction, int index)
        {
            if (TargetsLocalVariable(instruction.OpCode))
                return GetLocalVariable(index);
            return GetParameter(index);
        }

        private static bool TargetsLocalVariable(OpCode opcode)
        {
            return opcode.Name.Contains("loc");
        }

        private LocalVariableInfo GetLocalVariable(int index)
        {
            return locals[index];
        }

        private ParameterInfo GetParameter(int index)
        {
            if (!method.IsStatic)
                index--;

            return parameters[index];
        }

        private OpCode ReadOpCode()
        {
            var op = il.ReadByte();
            return op != 0xfe
                ? one_byte_opcodes[op]
                : two_bytes_opcodes[il.ReadByte()];
        }

        public static List<Instruction> GetInstructions(MethodBase method)
        {
            var reader = new MethodBodyReader(method);
            reader.ReadInstructions();
            return reader.instructions;
        }

        private class ByteBuffer
        {
            internal readonly byte[] buffer;
            internal int position;

            public ByteBuffer(byte[] buffer)
            {
                this.buffer = buffer;
            }

            public byte ReadByte()
            {
                CheckCanRead(1);
                return buffer[position++];
            }

            public byte[] ReadBytes(int length)
            {
                CheckCanRead(length);
                var value = new byte[length];
                Buffer.BlockCopy(buffer, position, value, 0, length);
                position += length;
                return value;
            }

            public short ReadInt16()
            {
                CheckCanRead(2);
                var value = (short)(buffer[position]
                                    | (buffer[position + 1] << 8));
                position += 2;
                return value;
            }

            public int ReadInt32()
            {
                CheckCanRead(4);
                var value = buffer[position]
                            | (buffer[position + 1] << 8)
                            | (buffer[position + 2] << 16)
                            | (buffer[position + 3] << 24);
                position += 4;
                return value;
            }

            public long ReadInt64()
            {
                CheckCanRead(8);
                var low = (uint)(buffer[position]
                                 | (buffer[position + 1] << 8)
                                 | (buffer[position + 2] << 16)
                                 | (buffer[position + 3] << 24));

                var high = (uint)(buffer[position + 4]
                                  | (buffer[position + 5] << 8)
                                  | (buffer[position + 6] << 16)
                                  | (buffer[position + 7] << 24));

                var value = ((long)high << 32) | low;
                position += 8;
                return value;
            }

            public float ReadSingle()
            {
                if (!BitConverter.IsLittleEndian)
                {
                    var bytes = ReadBytes(4);
                    Array.Reverse(bytes);
                    return BitConverter.ToSingle(bytes, 0);
                }

                CheckCanRead(4);
                var value = BitConverter.ToSingle(buffer, position);
                position += 4;
                return value;
            }

            public double ReadDouble()
            {
                if (!BitConverter.IsLittleEndian)
                {
                    var bytes = ReadBytes(8);
                    Array.Reverse(bytes);
                    return BitConverter.ToDouble(bytes, 0);
                }

                CheckCanRead(8);
                var value = BitConverter.ToDouble(buffer, position);
                position += 8;
                return value;
            }

            private void CheckCanRead(int count)
            {
                if (position + count > buffer.Length)
                    throw new ArgumentOutOfRangeException();
            }
        }
    }

    internal static class MethodBaseRocks
    {
        public static IList<Instruction> GetInstructions(this MethodBase self)
        {
            return MethodBodyReader.GetInstructions(self).AsReadOnly();
        }
    }
}