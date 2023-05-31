// Definition of custom attributes for declarative obfuscation.
// This file is only necessary for .NET Compact Framework and Silverlight projects.

using System.Reflection;
using System.Runtime.InteropServices;

[assembly: Obfuscation(Feature = "encrypt symbol names with password port_355", Exclude = false)]
#if UNITY3D
[assembly: Obfuscation(Feature = "Apply to ExpressionCompiler.*: all", Exclude = true, ApplyToMembers = true)]
[assembly: Obfuscation(Feature = "Apply to System.Linq.jvm.*: all", Exclude = true, ApplyToMembers = true)]
#endif

#if CF
#endif
namespace System.Reflection
{
    /// <summary>
    ///     Instructs obfuscation tools to use their standard obfuscation rules for the appropriate assembly type.
    /// </summary>
    [ComVisible(true)]
    [AttributeUsage(AttributeTargets.Assembly)]
    internal sealed class ObfuscateAssemblyAttribute : Attribute
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ObfuscateAssemblyAttribute" /> class,
        ///     specifying whether the assembly to be obfuscated is public or private.
        /// </summary>
        /// <param name="assemblyIsPrivate">
        ///     <c>true</c> if the assembly is used within the scope of one application; otherwise,
        ///     <c>false</c>.
        /// </param>
        public ObfuscateAssemblyAttribute(bool assemblyIsPrivate)
        {
            AssemblyIsPrivate = assemblyIsPrivate;
            StripAfterObfuscation = true;
        }

        /// <summary>
        ///     Gets a <see cref="System.Boolean" /> value indicating whether the assembly was marked private.
        /// </summary>
        /// <value>
        ///     <c>true</c> if the assembly was marked private; otherwise, <c>false</c>.
        /// </value>
        public bool AssemblyIsPrivate { get; }

        /// <summary>
        ///     Gets or sets a <see cref="System.Boolean" /> value indicating whether the obfuscation tool should remove the
        ///     attribute after processing.
        /// </summary>
        /// <value>
        ///     <c>true</c> if the obfuscation tool should remove the attribute after processing; otherwise, <c>false</c>.
        ///     The default value for this property is <c>true</c>.
        /// </value>
        public bool StripAfterObfuscation { get; set; }
    }

    /// <summary>
    ///     Instructs obfuscation tools to take the specified actions for an assembly, type, or member.
    /// </summary>
    [ComVisible(true)]
    [AttributeUsage(
        AttributeTargets.Delegate | AttributeTargets.Parameter | AttributeTargets.Interface | AttributeTargets.Event |
        AttributeTargets.Field | AttributeTargets.Property | AttributeTargets.Method | AttributeTargets.Enum |
        AttributeTargets.Struct | AttributeTargets.Class | AttributeTargets.Assembly, AllowMultiple = true,
        Inherited = false)]
    internal sealed class ObfuscationAttribute : Attribute
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ObfuscationAttribute" /> class.
        /// </summary>
        public ObfuscationAttribute()
        {
            ApplyToMembers = true;
            Exclude = true;
            Feature = "all";
            StripAfterObfuscation = true;
        }

        /// <summary>
        ///     Gets or sets a <see cref="System.Boolean" /> value indicating whether the attribute of a type is to apply to the
        ///     members of the type.
        /// </summary>
        /// <value>
        ///     <c>true</c> if the attribute is to apply to the members of the type; otherwise, <c>false</c>. The default is
        ///     <c>true</c>.
        /// </value>
        public bool ApplyToMembers { get; set; }

        /// <summary>
        ///     Gets or sets a <see cref="System.Boolean" /> value indicating whether the obfuscation tool should exclude the type
        ///     or member from obfuscation.
        /// </summary>
        /// <value>
        ///     <c>true</c> if the type or member to which this attribute is applied should be excluded from obfuscation;
        ///     otherwise, <c>false</c>.
        ///     The default is <c>true</c>.
        /// </value>
        public bool Exclude { get; set; }

        /// <summary>
        ///     Gets or sets a string value that is recognized by the obfuscation tool, and which specifies processing options.
        /// </summary>
        /// <value>
        ///     A string value that is recognized by the obfuscation tool, and which specifies processing options. The default is
        ///     "all".
        /// </value>
        public string Feature { get; set; }

        /// <summary>
        ///     Gets or sets a <see cref="System.Boolean" /> value indicating whether the obfuscation tool should remove the
        ///     attribute after processing.
        /// </summary>
        /// <value>
        ///     <c>true</c> if the obfuscation tool should remove the attribute after processing; otherwise, <c>false</c>.
        ///     The default value for this property is <c>true</c>.
        /// </value>
        public bool StripAfterObfuscation { get; set; }
    }
}