using System;
using System.Linq.Expressions;
using System.Linq.jvm;

namespace ExpressionCompiler
{
    public class ExpressionCompiler
    {
        public static Delegate Compile(LambdaExpression lambda)
        {
            if (lambda == null)
                throw new ArgumentNullException("lambda");

            return new Runner(lambda).CreateDelegate();
        }

        internal static Delegate Compile(LambdaExpression lambda, ExpressionInterpreter interpreter)
        {
            if (lambda == null)
                throw new ArgumentNullException("lambda");
            return new Runner(lambda, interpreter).CreateDelegate();
        }
    }
}