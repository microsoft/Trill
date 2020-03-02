// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
#if CODEGEN_TIMING
using System.Diagnostics;
#endif
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Emit;
using Microsoft.CodeAnalysis.Scripting.Hosting;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal static class Transformer
    {
        private static readonly bool IsNetCore = RuntimeInformation.FrameworkDescription.Contains(".NET Core");

        private static readonly Lazy<IEnumerable<MetadataReference>> baseAssemblyReferences
            = new Lazy<IEnumerable<MetadataReference>>(() => IsNetCore ? GetNetCoreAssemblyReferences() : GetNetFrameworkAssemblyReferences());
        private static readonly Func<MemoryStream, Assembly> AssemblyFromMemoryStream
            = IsNetCore ? (Func<MemoryStream, Assembly>)AssemblyFromMemoryStreamNetCore : AssemblyFromMemoryStreamNetFramework;
        private static readonly Func<string, Assembly> AssemblyFromFile
            = IsNetCore ? (Func<string, Assembly>)AssemblyFromFileNetCore : AssemblyFromFileNetFramework;

        // used so the compiler has access to the Microsoft.StramProcessing types it needs.
        // Fix this when there is a static location so we don't have to use Reflection to get it each time
        public static Assembly SystemRuntimeSerializationDll = typeof(System.Runtime.Serialization.DataContractAttribute).GetTypeInfo().Assembly;
        public static Assembly SystemDll = typeof(Uri).GetTypeInfo().Assembly;
        public static Assembly SystemCoreDll = typeof(BinaryExpression).GetTypeInfo().Assembly;

        /// <summary>
        /// This is used as part of constructing the name of a field in a StreamMessage that is a column representing a field of the payload type.
        /// </summary>
        internal const string ColumnFieldPrefix = "c_";

        /// <summary>
        /// Generate a batch class definition from the types <typeparamref name="TKey"/> and <typeparamref name="TPayload"/>.
        /// Compile the definition, dynamically load the assembly containing it, and return the Type representing the
        /// batch class.
        /// </summary>
        /// <typeparam name="TKey">
        /// The key type for the batch class.
        /// </typeparam>
        /// <typeparam name="TPayload">
        /// The payload type for which a batch class is generated. The batch class will have a field F of type T[] for every field
        /// F of type T in the type <typeparamref name="TPayload"/>.
        /// </typeparam>
        /// <returns>
        /// A type that is defined to be a subtype of Batch&lt;<typeparamref name="TKey"/>,<typeparamref name="TPayload"/>&gt;.
        /// </returns>
        public static Type GenerateBatchClass<TKey, TPayload>()
        {

#if CODEGEN_TIMING
            Stopwatch sw = new Stopwatch();
            sw.Start();
#endif

            var keyType = typeof(TKey);
            var payloadType = typeof(TPayload);
            SafeBatchTemplate.GetGeneratedCode(keyType, payloadType, out string generatedClassName, out string expandedCode, out List<Assembly> assemblyReferences);

            assemblyReferences.Add(MemoryManager.GetMemoryPool<TKey, TPayload>().GetType().GetTypeInfo().Assembly);
            assemblyReferences.Add(SystemRuntimeSerializationDll);
            if (keyType != typeof(Empty))
            {
                assemblyReferences.Add(typeof(Empty).GetTypeInfo().Assembly);
                assemblyReferences.Add(StreamMessageManager.GetStreamMessageType<Empty, TPayload>().GetTypeInfo().Assembly);
            }

            var a = CompileSourceCode(expandedCode, assemblyReferences, out string errorMessages);
            var t = a.GetType(generatedClassName);
            if (t.GetTypeInfo().IsGenericType)
            {
                var list = keyType.GetAnonymousTypes();
                list.AddRange(payloadType.GetAnonymousTypes());
                t = t.MakeGenericType(list.ToArray());
            }

#if CODEGEN_TIMING
            sw.Stop();
            Console.WriteLine("Time to generate and instantiate a batch for {0},{1}: {2}ms",
                keyType.GetCSharpSourceSyntax(), payload.GetCSharpSourceSyntax(), sw.ElapsedMilliseconds);
#endif
            return t;
        }

        /// <summary>
        /// Given a string, <paramref name="sourceCode"/>, that represents a compilable assembly, compile it into an assembly which is located
        /// in a sub-directory of the current working directory named "Generated", unless overriden by changing value of Config.GeneratedCodePath.
        /// If it is successful, then the resulting assembly is loaded and returned. Otherwise, null is returned and the
        /// parameter <paramref name="errorMessages"/> will contain the compiler errors.
        /// The parameter <paramref name="references"/> allows the client to specify the location of assemblies that are needed to compile
        /// the code.
        /// The parameter <paramref name="includeIgnoreAccessChecksAssembly"/> allows the generated assembly to reference
        /// the IgnoreAccessChecksTo attribute for access to Microsoft.StreamProcessing.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2001:AvoidCallingProblematicMethods", MessageId = "System.Reflection.GetTypeInfo().Assembly.LoadFrom", Justification = "There is no better way to load dynamically generated assembly.")]
        public static Assembly CompileSourceCode(string sourceCode, IEnumerable<Assembly> references, out string errorMessages, bool includeIgnoreAccessChecksAssembly = true)
        {
#if CODEGEN_TIMING
            Stopwatch sw = new Stopwatch();
            sw.Start();
#endif

            bool includeDebugInfo = Config.CodegenOptions.GenerateDebugInfo;
            var uniqueReferences = references.Distinct();

            if (includeIgnoreAccessChecksAssembly)
                uniqueReferences = uniqueReferences.Concat(new Assembly[] { IgnoreAccessChecks.Assembly });

            if (IsNetCore)
            {
                uniqueReferences = uniqueReferences.Where(r => !r.FullName.Contains("System.Private.CoreLib"));
            }

            var assemblyName = Path.GetFileNameWithoutExtension(Path.GetRandomFileName());

            SyntaxTree tree;

            if (includeDebugInfo)
            {
                if (!Directory.Exists(Config.GeneratedCodePath))
                {
                    Directory.CreateDirectory(Config.GeneratedCodePath); // let any exceptions bleed through
                }
                var baseFile = Path.Combine(Config.GeneratedCodePath, assemblyName);
                var sourceFile = Path.ChangeExtension(baseFile, ".cs");
                tree = CSharpSyntaxTree.ParseText(
                    sourceCode,
                    path: Path.GetFullPath(sourceFile),
                    encoding: Encoding.GetEncoding(0),
                    options: new CSharpParseOptions(LanguageVersion.Latest));
            }
            else
            {
                tree = CSharpSyntaxTree.ParseText(
                    sourceCode,
                    encoding: Encoding.GetEncoding(0),
                    options: new CSharpParseOptions(LanguageVersion.Latest));
            }

            MetadataReference trill = MetadataReference.CreateFromFile(typeof(StreamMessage).GetTypeInfo().Assembly.Location);
            var baseReferences = new List<MetadataReference>() { trill };

            var refs = baseReferences
                .Concat(baseAssemblyReferences.Value)
                .Concat(uniqueReferences.Where(r => Path.IsPathRooted(r.Location)).Select(r => MetadataReference.CreateFromFile(r.Location)))
                .Concat(uniqueReferences.Where(reference => metadataReferenceCache.ContainsKey(reference)).Select(reference => metadataReferenceCache[reference]));

            var options = new CSharpCompilationOptions(
                OutputKind.DynamicallyLinkedLibrary,
                metadataImportOptions: MetadataImportOptions.All,
                allowUnsafe: true,
                optimizationLevel: (includeDebugInfo ? OptimizationLevel.Debug : OptimizationLevel.Release));

            var topLevelBinderFlagsProperty = typeof(CSharpCompilationOptions).GetTypeInfo().GetProperty("TopLevelBinderFlags", BindingFlags.Instance | BindingFlags.NonPublic);
            var binderFlagsType = typeof(CSharpCompilationOptions).GetTypeInfo().Assembly.GetType("Microsoft.CodeAnalysis.CSharp.BinderFlags");
            var ignoreCorLibraryDuplicatedTypesMember = binderFlagsType.GetTypeInfo().GetField("IgnoreCorLibraryDuplicatedTypes", BindingFlags.Static | BindingFlags.Public);
            var ignoreAccessibility = binderFlagsType.GetTypeInfo().GetField("IgnoreAccessibility", BindingFlags.Static | BindingFlags.Public);
            topLevelBinderFlagsProperty.SetValue(options, (uint)ignoreCorLibraryDuplicatedTypesMember.GetValue(null) | (uint)ignoreAccessibility.GetValue(null));

            SyntaxTree[] trees = { tree };
            var compilation = CSharpCompilation.Create(assemblyName, trees, refs, options);
            var assembly = EmitCompilationAndLoadAssembly(compilation, includeDebugInfo, out errorMessages);

#if CODEGEN_TIMING
            sw.Stop();
            Console.WriteLine("Time to compile: {0}ms", sw.ElapsedMilliseconds);
#endif
            return assembly;
        }

        public static ConcurrentDictionary<Assembly, MetadataReference> metadataReferenceCache = new ConcurrentDictionary<Assembly, MetadataReference>();
        private static readonly InteractiveAssemblyLoader loader = new InteractiveAssemblyLoader();

        internal static Assembly EmitCompilationAndLoadAssembly(CSharpCompilation compilation, bool makeAssemblyDebuggable, out string errorMessages)
        {
            Contract.Requires(compilation.SyntaxTrees.Count() == 1);

            Assembly assembly = null;
            EmitResult emitResult;

            if (makeAssemblyDebuggable)
            {
                var tree = compilation.SyntaxTrees.Single();
                var baseFile = tree.FilePath;
                var sourceFile = Path.ChangeExtension(baseFile, ".cs");
                File.WriteAllText(sourceFile, tree.GetRoot().ToFullString());
                var assemblyFile = Path.ChangeExtension(baseFile, ".dll");
                using (var assemblyStream = File.Open(assemblyFile, FileMode.Create, FileAccess.Write))
                using (var pdbStream = File.Open(Path.ChangeExtension(baseFile, ".pdb"), FileMode.Create, FileAccess.Write))
                {
                    emitResult = compilation.Emit(assemblyStream, pdbStream, options: new EmitOptions(debugInformationFormat: DebugInformationFormat.PortablePdb));
                }

                if (emitResult.Success)
                {
                    assembly = AssemblyFromFile(assemblyFile);
                }
            }
            else
            {
                using (var stream = new MemoryStream())
                {
                    emitResult = compilation.Emit(stream);
                    if (emitResult.Success)
                    {
                        stream.Position = 0;
                        assembly = AssemblyFromMemoryStream(stream);

                        loader.RegisterDependency(assembly);
                        var aref = MetadataReference.CreateFromStream(stream);
                        metadataReferenceCache.TryAdd(assembly, aref);
                    }
                }
            }

            errorMessages = string.Join("\n", emitResult.Diagnostics);

            if (makeAssemblyDebuggable && emitResult.Diagnostics.Any(d => d.Severity == DiagnosticSeverity.Error))
                System.Diagnostics.Debug.WriteLine(errorMessages);

            return assembly;
        }

        #region netcore

        internal static IEnumerable<PortableExecutableReference> GetNetCoreAssemblyReferences()
        {
            var allAvailableAssemblies = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES"))
                .Split(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? ';' : ':');

            // From: https://github.com/dotnet/roslyn/blob/master/src/Interactive/csi/csi.coreclr.rsp
            // These references are resolved lazily. Keep in sync with list in core csi.coreslr.rsp.
            var files = new[]
            {
                "System.Collections",
                "System.Collections.Concurrent",
                "System.Console",
                "System.Diagnostics.Debug",
                "System.Diagnostics.Process",
                "System.Diagnostics.StackTrace",
                "System.Globalization",
                "System.IO",
                "System.IO.FileSystem",
                "System.IO.FileSystem.Primitives",
                "System.Linq",
                "System.Linq.Expressions",
                "System.Reflection",
                "System.Reflection.Extensions",
                "System.Reflection.Primitives",
                "System.Runtime",
                "System.Runtime.Extensions",
                "System.Runtime.InteropServices",
                "System.Text.Encoding",
                "System.Text.Encoding.CodePages",
                "System.Text.Encoding.Extensions",
                "System.Text.RegularExpressions",
                "System.Threading",
                "System.Threading.Tasks",
                "System.Threading.Tasks.Parallel",
                "System.Threading.Thread",
            };
            var filteredPaths = allAvailableAssemblies.Where(p => files.Concat(new string[] { "mscorlib", "netstandard", "System.Private.CoreLib", "System.Runtime.Serialization.Primitives", }).Any(f => Path.GetFileNameWithoutExtension(p).Equals(f)));
            return filteredPaths.Select(p => MetadataReference.CreateFromFile(p));
        }

        // Important - System.Runtime.Loader should not be referenced by any function that can run in a net framework environment!
        internal static Assembly AssemblyFromMemoryStreamNetCore(MemoryStream stream)
        {
            var assembly = System.Runtime.Loader.AssemblyLoadContext.Default.LoadFromStream(stream);
            stream.Position = 0; // Must reset it! Loading leaves its position at the end
            return assembly;
        }

        internal static Assembly AssemblyFromFileNetCore(string file) => System.Runtime.Loader.AssemblyLoadContext.Default.LoadFromAssemblyPath(file);

        #endregion netcore

        #region netframework

        internal static IEnumerable<MetadataReference> GetNetFrameworkAssemblyReferences()
        {
            MetadataReference mscorlib = MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location);
            MetadataReference numerics = MetadataReference.CreateFromFile(typeof(System.Numerics.Complex).GetTypeInfo().Assembly.Location);
            MetadataReference linq = MetadataReference.CreateFromFile(typeof(Enumerable).GetTypeInfo().Assembly.Location);
            MetadataReference contracts = MetadataReference.CreateFromFile(typeof(System.Runtime.Serialization.DataContractAttribute).GetTypeInfo().Assembly.Location);

            // If we are compiling a netstandard binary from net framework environment, we have to explicitly add a reference to netstandard
            var netstandardPath = Path.Combine(Path.GetDirectoryName(typeof(object).GetTypeInfo().Assembly.Location), "netstandard.dll");
            MetadataReference netstandard = MetadataReference.CreateFromFile(netstandardPath);

            return new MetadataReference[] { mscorlib, numerics, linq, contracts, netstandard };
        }

        internal static Assembly AssemblyFromMemoryStreamNetFramework(MemoryStream stream) => Assembly.Load(stream.ToArray());

        internal static Assembly AssemblyFromFileNetFramework(string file) => Assembly.LoadFrom(file);

        #endregion netframework

        internal static IEnumerable<Assembly> AssemblyReferencesNeededForType(Type type)
        {
            var closure = new HashSet<Type>();
            CollectAssemblyReferences(type, closure);
            var result = closure
                .Select(t => t.GetTypeInfo().Assembly)
                .Where(t => !t.IsDynamic)
                .Distinct();
            return result;
        }

        internal static void CollectAssemblyReferences(Type t, HashSet<Type> partialClosure)
        {
            if (partialClosure.Add(t))
            {
                if (t.GetTypeInfo().BaseType != null)
                    CollectAssemblyReferences(t.GetTypeInfo().BaseType, partialClosure);
                if (t.IsNested)
                    CollectAssemblyReferences(t.DeclaringType, partialClosure);

                foreach (var j in t.GetTypeInfo().GetInterfaces())
                    CollectAssemblyReferences(j, partialClosure);
                foreach (var genericArgument in t.GenericTypeArguments)
                    CollectAssemblyReferences(genericArgument, partialClosure);
                foreach (var f in t.GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance))
                    CollectAssemblyReferences(f.FieldType, partialClosure);
                foreach (var p in t.GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance))
                    CollectAssemblyReferences(p.PropertyType, partialClosure);
            }
        }

        public static IEnumerable<Assembly> AssemblyReferencesNeededFor(Expression expression)
            => AssemblyLocationFinder.GetAssemblyLocationsFor(expression);

        public static IEnumerable<Assembly> AssemblyReferencesNeededFor(params Expression[] expressions)
        {
            var assemblies = new HashSet<Assembly>();
            foreach (var expression in expressions)
            {
                foreach (var assembly in AssemblyLocationFinder.GetAssemblyLocationsFor(expression))
                {
                    assemblies.Add(assembly);
                }
            }

            return assemblies;
        }

        private sealed class AssemblyLocationFinder : ExpressionVisitor
        {
            private readonly HashSet<Assembly> assemblyLocations = new HashSet<Assembly>();

            private AssemblyLocationFinder() { }
            public static IEnumerable<Assembly> GetAssemblyLocationsFor(Expression e)
            {
                var me = new AssemblyLocationFinder();
                me.Visit(e);
                return me.assemblyLocations;
            }
            protected override Expression VisitMember(MemberExpression node)
            {
                var a = node.Member.DeclaringType.GetTypeInfo().Assembly;
                this.assemblyLocations.Add(a);
                return base.VisitMember(node);
            }
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                var method = node.Method;
                if (method.IsStatic)
                {
                    var a = method.DeclaringType.GetTypeInfo().Assembly;
                    this.assemblyLocations.Add(a);
                }
                return base.VisitMethodCall(node);
            }
        }

        // Lazy Singleton to compile the IgnoreAccessChecksToAttribute, so codegen assemblies can still access
        // Microsoft.StreamProcessing internals
        internal static class IgnoreAccessChecks
        {
            private const string IgnoreAccessChecksSourceCode = @"
namespace System.Runtime.CompilerServices
{
    [AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
    public class IgnoresAccessChecksToAttribute : Attribute
    {
        public IgnoresAccessChecksToAttribute(string assemblyName)
        {
            AssemblyName = assemblyName;
        }

        public string AssemblyName { get; }
    }
}";

            internal static Assembly Assembly => lazySingleton.Value;

            private static readonly Lazy<Assembly> lazySingleton = new Lazy<Assembly>(() => CreateIgnoreAccessChecksAssembly());

            private static Assembly CreateIgnoreAccessChecksAssembly()
            {
                var assembly = CompileSourceCode(IgnoreAccessChecksSourceCode, Array.Empty<Assembly>(), out _, false);
                if (assembly == null)
                {
                    throw new InvalidOperationException("Code Generation failed for IgnoresAccessChecksToAttribute!");
                }
                return assembly;
            }
        }

        private static int BatchClassSequenceNumber = 0;
        private static readonly SafeConcurrentDictionary<string> batchType2Name = new SafeConcurrentDictionary<string>();

        internal static string GetBatchClassName(Type keyType, Type payloadType)
        {
            if (!payloadType.CanRepresentAsColumnar())
                return $"StreamMessage<{keyType.GetCSharpSourceSyntax()}, {payloadType.GetCSharpSourceSyntax()}>";

            var dictionaryKey = CacheKey.Create(keyType, payloadType);
            return batchType2Name.GetOrAdd(
                dictionaryKey,
                key => $"GeneratedBatch_{BatchClassSequenceNumber++}");
        }

        internal static string GetMemoryPoolClassName(Type keyType, Type payloadType)
        {
            if (!keyType.KeyTypeNeedsGeneratedMemoryPool() && payloadType.MemoryPoolHasGetMethodFor())
                return $"MemoryPool<{keyType.GetCSharpSourceSyntax()}, {payloadType.GetCSharpSourceSyntax()}>";

            if (!payloadType.CanRepresentAsColumnar())
                return $"MemoryPool<{keyType.GetCSharpSourceSyntax()}, {payloadType.GetCSharpSourceSyntax()}>";

            return $"MemoryPool_{keyType.Name.CleanUpIdentifierName()}_{payloadType.Name.CleanUpIdentifierName()}";
        }

        internal static string GetValidIdentifier(Type t) => t.GetCSharpSourceSyntax().CleanUpIdentifierName();

        internal static List<Assembly> AssemblyReferencesNeededFor(params Type[] ts)
        {
            var result = new List<Assembly>();
            foreach (var t in ts)
                result.AddRange(AssemblyReferencesNeededForType(t));
            return result;
        }

        internal static string GenericParameterList(params string[] ps)
        {
            var validStrings = ps.Where(x => !string.IsNullOrWhiteSpace(x));
            var commaSeparatedList = string.Join(",", validStrings);
            return string.IsNullOrWhiteSpace(commaSeparatedList) ? string.Empty : "<" + commaSeparatedList + ">";
        }

        internal static Type GenerateMemoryPoolClass<TKey, TPayload>()
        {
            Contract.Ensures(Contract.Result<Type>() != null);
            Contract.Ensures(typeof(MemoryPool<TKey, TPayload>).GetTypeInfo().IsAssignableFrom(Contract.Result<Type>()));

#if CODEGEN_TIMING
            Stopwatch sw = new Stopwatch();
            sw.Start();
#endif

            string generatedClassName;
            string expandedCode;
            List<Assembly> assemblyReferences;

            var keyType = typeof(TKey);
            var payloadType = typeof(TPayload);
            var mpt = new MemoryPoolTemplate(new ColumnarRepresentation(keyType), new ColumnarRepresentation(payloadType));
            expandedCode = mpt.expandedCode;
            assemblyReferences = mpt.assemblyReferences;
            generatedClassName = mpt.generatedClassName;

            var a = CompileSourceCode(expandedCode, assemblyReferences, out string errorMessages);

            var t = a.GetType(generatedClassName);
            var instantiatedType = t.MakeGenericType(new Type[] { keyType, payloadType });
#if CODEGEN_TIMING
            sw.Stop();
            Console.WriteLine("Time to generate and instantiate a memory pool for {0},{1}: {2}ms",
                tKey.GetCSharpSourceSyntax(), tPayload.GetCSharpSourceSyntax(), sw.ElapsedMilliseconds);
#endif
            return instantiatedType;
        }

        /// <summary>
        /// A type is a valid key type (i.e., it can be used as a Key type for a StreamMessage)
        /// if it is any type T that is not a CompoundGroupKey or is a valid CGK.
        /// </summary>
        internal static bool IsValidKeyType(Type t)
            => !t.GetTypeInfo().IsGenericType || t.GetGenericTypeDefinition() != typeof(CompoundGroupKey<,>) || IsValidCGK(t);

        /// <summary>
        /// A type is a valid CGK (i.e., it can be used as a Key type for a StreamMessage)
        /// as long as it is a left-branching structure. The base case can be any type.
        /// E.g., any type T (that is not a CompoundGroupKey) is a valid CGK.
        /// Or it can be CGK of T1, T2 where T2 is not a CompoundGroupKey and T1 is a valid key type.
        /// </summary>
        private static bool IsValidCGK(Type t)
        {
            Contract.Requires(t.GetTypeInfo().IsGenericType && t.GetGenericTypeDefinition() == typeof(CompoundGroupKey<,>));

            var typeArgs = t.GetTypeInfo().GetGenericArguments();
            var innerKeyType = typeArgs[1];
            return (!innerKeyType.GetTypeInfo().IsGenericType || innerKeyType.GetGenericTypeDefinition() != typeof(CompoundGroupKey<,>)) && IsValidKeyType(typeArgs[0]);
        }

        public static Assembly GeneratedStreamMessageAssembly<TKey, TPayload>()
            => StreamMessageManager.GetStreamMessageType<TKey, TPayload>().GetTypeInfo().Assembly;

        public static Assembly GeneratedMemoryPoolAssembly<TKey, TPayload>()
            => MemoryManager.GetMemoryPool<TKey, TPayload>().GetType().GetTypeInfo().Assembly;
    }

    internal sealed class TypeMapper
    {
        private readonly Dictionary<Type, string> typeMap;

        public TypeMapper(params Type[] types) => this.typeMap = GetCSharpTypeNames(types);

        public string CSharpNameFor(Type t) => this.typeMap[t];

        public IEnumerable<string> GenericTypeVariables(params Type[] types)
        {
            var l = new List<string>();
            foreach (var t in types)
            {
                if (t.IsAnonymousTypeName())
                {
                    l.Add(this.typeMap[t]);
                    continue;
                }

                if (!t.GetTypeInfo().Assembly.IsDynamic && t.GetTypeInfo().IsGenericType)
                {
                    foreach (var gta in t.GenericTypeArguments) l.AddRange(GenericTypeVariables(gta));
                }
            }
            return l.Distinct();
        }

        private static Dictionary<Type, string> GetCSharpTypeNames(params Type[] types)
        {
            var d = new Dictionary<Type, string>();
            var anonTypeCount = 0;
            for (int i = 0; i < types.Length; i++)
                TurnTypeIntoCSharpSourceHelper(types[i], d, ref anonTypeCount);
            return d;
        }

        private static void TurnTypeIntoCSharpSourceHelper(Type t, Dictionary<Type, string> d, ref int anonymousTypeCount)
        {
            Contract.Requires(t != null);
            Contract.Requires(d != null);
            if (d.TryGetValue(t, out _)) return;

            var typeName = t.FullName.Replace('#', '_').Replace('+', '.');
            if (t.IsAnonymousTypeName())
            {
                var newGenericTypeParameter = "A" + anonymousTypeCount.ToString(CultureInfo.InvariantCulture);
                anonymousTypeCount++;
                d.Add(t, newGenericTypeParameter);
                return;
            }
            if (!t.GetTypeInfo().IsGenericType) // need to test after anonymous because deserialized anonymous types are *not* generic (but unserialized anonymous types *are* generic)
            { d.Add(t, typeName); return; }
            var sb = new StringBuilder();
            typeName = typeName.Substring(0, t.FullName.IndexOf('`'));
            sb.AppendFormat("{0}<", typeName);
            var first = true;
            if (!t.GetTypeInfo().Assembly.IsDynamic)
            {
                foreach (var genericArgument in t.GenericTypeArguments)
                {
                    TurnTypeIntoCSharpSourceHelper(genericArgument, d, ref anonymousTypeCount);
                    string name = d[genericArgument];
                    if (!first) sb.Append(", ");
                    sb.Append(name);
                    first = false;
                }
            }
            sb.Append(">");
            typeName = sb.ToString();
            d.Add(t, typeName);
            return;
        }
    }

    internal sealed class ColumnarRepresentation
    {
        public readonly Type RepresentationFor;
        public readonly IDictionary<string, MyFieldInfo> Fields; // keyed by field name
        public readonly bool noFields;
        public readonly MyFieldInfo PseudoField; // used only when noFields is true

        public IEnumerable<MyFieldInfo> AllFields => this.noFields
                    ? new List<MyFieldInfo>() { this.PseudoField }
                    : this.Fields.Values;

        public ColumnarRepresentation(Type t)
        {
            this.RepresentationFor = t;
            var d = new Dictionary<string, MyFieldInfo>();
            this.Fields = d;
            foreach (var f in t.GetTypeInfo().GetFields(BindingFlags.Instance | BindingFlags.Public))
                d.Add(f.Name, new MyFieldInfo(f/*, prefix*/));

            // Any autoprops should be treated just as if they were a field
            foreach (var p in t.GetTypeInfo().GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                var getMethod = p.GetMethod;
                if (getMethod == null) continue;
                if (!getMethod.IsDefined(typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute))) continue;
                var setMethod = p.SetMethod;
                if (setMethod == null) continue;
                if (!setMethod.IsDefined(typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute))) continue;

                d.Add(p.Name, new MyFieldInfo(p/*, prefix*/));
            }

            if (!this.Fields.Any())
            {
                if (t.HasSupportedParameterizedConstructor())
                {
                    foreach (var p in t.GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance))
                    {
                        d.Add(p.Name, new MyFieldInfo(p/*, prefix*/));
                    }
                }
                else
                {
                    this.noFields = true;
                    this.PseudoField = new MyFieldInfo(t, "payload");
                }
            }
        }

        public ColumnarRepresentation(Type t, string pseudoFieldName)
        {
            this.RepresentationFor = t;
            this.noFields = true;
            this.PseudoField = new MyFieldInfo(t, pseudoFieldName);
        }
    }

    internal struct MyFieldInfo
    {
        public Type Type;
        public Type DeclaringType;
        public string TypeName;
        public string Name;
        public bool canBeFixed;
        public string OriginalName;
        public bool isField;

        public MyFieldInfo(FieldInfo f)
        {
            this.Type = f.FieldType;
            this.TypeName = f.FieldType.GetCSharpSourceSyntax();
            this.Name = Transformer.ColumnFieldPrefix + f.Name;
            this.OriginalName = f.Name;
            var t = f.FieldType;
            this.canBeFixed = t.CanBeFixed();
            this.isField = true;
            this.DeclaringType = f.DeclaringType;
        }

        /// <summary>
        /// Used only for anonymous types and autoprops (REVIEW: should be enforced, at least with a contract)
        /// </summary>
        public MyFieldInfo(PropertyInfo p)
        {
            var t = p.PropertyType;
            this.Type = t;
            this.TypeName = t.GetCSharpSourceSyntax();
            this.Name = Transformer.ColumnFieldPrefix + p.Name;
            this.OriginalName = p.Name;
            this.canBeFixed = t.CanBeFixed();
            this.isField = false;
            this.DeclaringType = p.DeclaringType;
        }

        public MyFieldInfo(Type t, string name = "payload")
        {
            this.Type = t;
            this.TypeName = t.GetCSharpSourceSyntax();
            this.Name = name;
            this.OriginalName = "payload";
            this.canBeFixed = t.CanBeFixed();
            this.isField = false;
            this.DeclaringType = t.DeclaringType;
        }

        public override string ToString() => "|" + this.TypeName + ", " + this.Name + "|";
    }

    internal partial class SafeBatchTemplate
    {
        private string CLASSNAME;
        private IEnumerable<MyFieldInfo> fields;
        private Type payloadType;
        private Type keyType;

        /// <summary>
        /// When the payload type doesn't have any public fields (e.g., it is a primitive type like int64),
        /// then a pseudo-field, "payload" is used to hold full payload, as opposed to its public fields
        /// being turned into fields in the generated batch class.
        /// </summary>
        private bool noPublicFields;
        private bool needsPolymorphismCheck = false;
        private bool payloadMightBeNull = false;

        private SafeBatchTemplate() { }

        public static void GetGeneratedCode(Type keyType, Type payloadType, out string generatedClassName, out string expandedCode, out List<Assembly> assemblyReferences)
        {
            var template = new SafeBatchTemplate();

            assemblyReferences = new List<Assembly>
            {
                Transformer.SystemRuntimeSerializationDll // needed for [DataContract] and [DataMember] in generated code
            };

            assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keyType));
            template.keyType = keyType;

            assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(payloadType));
            template.payloadType = payloadType;
            template.needsPolymorphismCheck =
                !payloadType.GetTypeInfo().IsValueType &&
                !payloadType.IsAnonymousTypeName() &&
                !payloadType.GetTypeInfo().IsSealed;
            template.payloadMightBeNull = payloadType.CanContainNull();

            generatedClassName = Transformer.GetBatchClassName(keyType, payloadType);
            template.CLASSNAME = generatedClassName.CleanUpIdentifierName();
            generatedClassName = generatedClassName.AddNumberOfNecessaryGenericArguments(keyType, payloadType);

            var payloadRepresentation = new ColumnarRepresentation(payloadType);
            template.fields = payloadRepresentation.AllFields;
            template.noPublicFields = payloadRepresentation.noFields;

            expandedCode = template.TransformText();
        }
    }

    internal partial class MemoryPoolTemplate
    {
        public string generatedClassName;
        public string expandedCode;
        public List<Assembly> assemblyReferences;

        private readonly Type keyType;
        private readonly Type payloadType;

        /// <summary>
        /// A set so that there is just one memory pool and Get method per type.
        /// </summary>
        private readonly HashSet<Type> types;

        private readonly string className;

        internal MemoryPoolTemplate(ColumnarRepresentation keyRepresentation, ColumnarRepresentation payloadRepresentation)
        {
            this.assemblyReferences = new List<Assembly>();

            var keyType = keyRepresentation == null ? typeof(Empty) : keyRepresentation.RepresentationFor;

            Contract.Assume(Transformer.IsValidKeyType(keyType));

            this.assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keyType));
            this.keyType = keyType;

#region Decompose TPayload into columns
            var payloadType = payloadRepresentation.RepresentationFor;
            this.assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(payloadType));
            this.payloadType = payloadType;
            var payloadTypes = payloadRepresentation.AllFields.Select(f => f.Type).Where(t => !t.MemoryPoolHasGetMethodFor());

            this.types = new HashSet<Type>(payloadTypes.Distinct());

            this.assemblyReferences.AddRange(this.types.SelectMany(t => Transformer.AssemblyReferencesNeededFor(t)));
#endregion

            this.generatedClassName = Transformer.GetMemoryPoolClassName(keyType, payloadType);
            this.className = this.generatedClassName.CleanUpIdentifierName();
            this.generatedClassName += "`2";
            this.expandedCode = TransformText();
        }
    }
}