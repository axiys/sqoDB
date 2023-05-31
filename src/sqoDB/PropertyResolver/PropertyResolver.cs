﻿using System;
using System.Reflection;
using System.Reflection.Emit;
using Mono.Reflection;

namespace sqoDB.PropertyResolver
{
    //
    // BackingFieldResolver.cs
    //
    // Author:
    // Jb Evain (jbevain@novell.com)
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


    internal abstract class ILPattern
    {
        public static ILPattern Optional(OpCode opcode)
        {
            return Optional(OpCode(opcode));
        }

        public static ILPattern Optional(ILPattern pattern)
        {
            return new OptionalPattern(pattern);
        }

        public static ILPattern Sequence(params ILPattern[] patterns)
        {
            return new SequencePattern(patterns);
        }

        public static ILPattern OpCode(OpCode opcode)
        {
            return new OpCodePattern(opcode);
        }

        public static ILPattern Either(ILPattern a, ILPattern b)
        {
            return new EitherPattern(a, b);
        }

        public static ILPattern Field(OpCode opcode)
        {
            return new FieldPattern(new OpCodePattern(opcode));
        }

        internal abstract void Match(MatchContext context);

        internal bool TryMatch(MatchContext context)
        {
            var instruction = context.instruction;
            Match(context);

            if (context.success)
                return true;

            context.Reset(instruction);
            return false;
        }

        public static MatchContext Match(MethodBase method, ILPattern pattern)
        {
            var instructions = method.GetInstructions();
            if (instructions.Count == 0)
                throw new ArgumentException();

            var context = new MatchContext(instructions[0]);
            pattern.Match(context);
            return context;
        }

        private class OptionalPattern : ILPattern
        {
            private readonly ILPattern pattern;

            public OptionalPattern(ILPattern optional)
            {
                pattern = optional;
            }

            internal override void Match(MatchContext context)
            {
                pattern.TryMatch(context);
            }
        }

        private class SequencePattern : ILPattern
        {
            private readonly ILPattern[] patterns;

            public SequencePattern(ILPattern[] patterns)
            {
                this.patterns = patterns;
            }

            internal override void Match(MatchContext context)
            {
                foreach (var pattern in patterns)
                {
                    pattern.Match(context);

                    if (!context.success)
                        break;
                }
            }
        }

        private class OpCodePattern : ILPattern
        {
            private readonly OpCode opcode;

            public OpCodePattern(OpCode opcode)
            {
                this.opcode = opcode;
            }

            internal override void Match(MatchContext context)
            {
                if (context.instruction == null)
                {
                    context.success = false;
                    return;
                }

                context.success = context.instruction.OpCode == opcode;
                context.Advance();
            }
        }

        private class EitherPattern : ILPattern
        {
            private readonly ILPattern a;
            private readonly ILPattern b;

            public EitherPattern(ILPattern a, ILPattern b)
            {
                this.a = a;
                this.b = b;
            }

            internal override void Match(MatchContext context)
            {
                if (!a.TryMatch(context))
                    b.Match(context);
            }
        }

        private class FieldPattern : ILPattern
        {
            private readonly ILPattern pattern;

            public FieldPattern(ILPattern pattern)
            {
                this.pattern = pattern;
            }

            internal override void Match(MatchContext context)
            {
                if (!pattern.TryMatch(context))
                {
                    context.success = false;
                    return;
                }

                context.field = (FieldInfo)context.instruction.Previous.Operand;
            }
        }
    }

    internal class MatchContext
    {
        internal FieldInfo field;

        internal Instruction instruction;
        internal bool success;

        public MatchContext(Instruction instruction)
        {
            Reset(instruction);
        }

        public void Reset(Instruction instruction)
        {
            this.instruction = instruction;
            success = true;
        }

        public void Advance()
        {
            instruction = instruction.Next;
        }
    }

    internal class BackingFieldResolver
    {
        private static readonly ILPattern GetterPattern =
            ILPattern.Sequence(
                ILPattern.Optional(OpCodes.Nop),
                ILPattern.Either(
                    ILPattern.Field(OpCodes.Ldsfld),
                    ILPattern.Sequence(
                        ILPattern.OpCode(OpCodes.Ldarg_0),
                        ILPattern.Field(OpCodes.Ldfld))),
                ILPattern.Optional(
                    ILPattern.Sequence(
                        ILPattern.OpCode(OpCodes.Stloc_0),
                        ILPattern.OpCode(OpCodes.Br_S),
                        ILPattern.OpCode(OpCodes.Ldloc_0))),
                ILPattern.OpCode(OpCodes.Ret));

        private static readonly ILPattern SetterPattern =
            ILPattern.Sequence(
                ILPattern.Optional(OpCodes.Nop),
                ILPattern.OpCode(OpCodes.Ldarg_0),
                ILPattern.Either(
                    ILPattern.Field(OpCodes.Stsfld),
                    ILPattern.Sequence(
                        ILPattern.OpCode(OpCodes.Ldarg_1),
                        ILPattern.Field(OpCodes.Stfld))),
                ILPattern.OpCode(OpCodes.Ret));

        private static FieldInfo GetBackingField(MethodInfo method, ILPattern pattern)
        {
            //Debug.WriteLine("Enter on Reflection.EMIT and works!");
            var result = ILPattern.Match(method, pattern);
            if (!result.success)
                throw new NotSupportedException();
            // Debug.WriteLine("Exit on Reflection.EMIT and works!");
            return result.field;
        }

        public static FieldInfo GetBackingField(PropertyInfo self)
        {
            //Debug.WriteLine("Enter on Reflection.EMIT and works!");
            var getter = self.GetGetMethod(true);
            if (getter != null)
                return GetBackingField(getter, GetterPattern);

            var setter = self.GetSetMethod(true);
            if (setter != null)
                return GetBackingField(setter, SetterPattern);

            throw new ArgumentException();
        }
    }
}