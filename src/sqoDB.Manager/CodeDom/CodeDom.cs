using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Reflection;
using System.Text;
using Microsoft.CSharp;

namespace sqoDB.Manager
{
    public class CodeDom
    {
        private readonly List<CodeNamespace> listNamespaces = new List<CodeNamespace>();

        private readonly StringCollection listReferencedAssemblies =
            new StringCollection { "System.dll" };

        private List<CodeCompileUnit> listCompileUnits = new List<CodeCompileUnit>();

        public CodeCompileUnit CompileUnit
        {
            get
            {
                var compileUnit = new CodeCompileUnit();

                foreach (var ns in listNamespaces)
                    compileUnit.Namespaces.Add(ns);

                return compileUnit;
            }
        }


        public static CodeDomProvider Provider()
        {
            var providerOptions = new Dictionary<string, string>();
            providerOptions.Add("CompilerVersion", "v3.5");

            return new CSharpCodeProvider(providerOptions);
        }

        public CodeNamespace AddNamespace(string namespaceName)
        {
            var codeNamespace = new CodeNamespace(namespaceName);
            listNamespaces.Add(codeNamespace);

            return codeNamespace;
        }

        public CodeDom AddReference(string referencedAssembly)
        {
            listReferencedAssemblies.Add(referencedAssembly);

            return this;
        }

        public CodeTypeDeclaration Class(string className)
        {
            return new CodeTypeDeclaration(className);
        }

        public CodeSnippetTypeMember Method(string returnType, string methodName, string paramList, string methodBody)
        {
            return Method(string.Format("public static {0} {1}({2}) {{ {3} }} ", returnType, methodName, paramList,
                methodBody));
        }

        public CodeSnippetTypeMember Method(string methodName, string paramList, string methodBody)
        {
            return Method("void", methodName, paramList, methodBody);
        }

        public CodeSnippetTypeMember Method(string methodName, string methodBody)
        {
            return Method("void", methodName, "", methodBody);
        }

        public CodeSnippetTypeMember Method(string methodBody)
        {
            return new CodeSnippetTypeMember(methodBody);
        }

        public Assembly Compile(OutputErrors renderErrors)
        {
            return Compile(null, renderErrors);
        }

        public Assembly Compile(string assemblyPath, OutputErrors renderErrors)
        {
            var options = new CompilerParameters();
            options.IncludeDebugInformation = false;
            options.GenerateExecutable = false;
            options.GenerateInMemory = assemblyPath == null;
            foreach (var refAsm in listReferencedAssemblies)
                options.ReferencedAssemblies.Add(refAsm);
            if (assemblyPath != null)
                options.OutputAssembly = assemblyPath.Replace('\\', '/');

            var codeProvider = Provider();

            var results =
                codeProvider.CompileAssemblyFromDom(options, CompileUnit);
            codeProvider.Dispose();

            if (results.Errors.Count == 0)
                return results.CompiledAssembly;


            renderErrors("Errors:");

            foreach (CompilerError err in results.Errors)
                renderErrors(err.ToString());

            return null;
        }

        public string GenerateCode()
        {
            var sb = new StringBuilder();
            TextWriter tw = new IndentedTextWriter(new StringWriter(sb));

            var codeProvider = Provider();
            codeProvider.GenerateCodeFromCompileUnit(CompileUnit, tw, new CodeGeneratorOptions());
            codeProvider.Dispose();

            tw.Close();

            return sb.ToString();
        }
    }

    public delegate void OutputErrors(string errorLine);
}