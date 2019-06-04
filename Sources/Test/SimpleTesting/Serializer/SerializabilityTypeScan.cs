// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class SerializabilityTypeScanTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void Serializability_ApiScan_Test()
        {
            bool foundBadTypes = false;

            foreach (var assemblyName in Assemblies.OrderBy(e => e, StringComparer.Ordinal))
            {
                var assembly = Assembly.Load(assemblyName);
                foreach (var g in from t in assembly.GetTypes()
                                  where t.IsPublic
                                  group t by t.Namespace into g
                                  orderby g.Key
                                  select g)
                {
                    Console.WriteLine("==> Scanning namespace: {0}", g.Key);
                    foreach (var type in from t in g
                                         where !SkipTypes(t)
                                         orderby t.Name
                                         select t)
                    {
                        string name;
                        Type testType;
                        if (type.IsGenericTypeDefinition)
                        {
                            name = DescribeGeneric(type.FullName, type.GetGenericArguments());
                            var hasConstraints = type.GetGenericArguments()
                                .Any(e => e.GetGenericParameterConstraints().Length > 0);
                            if (hasConstraints)
                            {
                                // Specialcase generic types with type constraints
                                foundBadTypes = true;
                                DescribeTestOutcome(name, false);
                                System.Diagnostics.Debug.WriteLine("\t!!!! Creating instances of generic types with type constraints is not supported.");
                                continue;
                            }
                            var typeArgs = type.GetGenericArguments()
                                .Select(_ => typeof(int))
                                .ToArray();
                            testType = type.GetGenericTypeDefinition().MakeGenericType(typeArgs);
                        }
                        else
                        {
                            name = type.FullName;
                            testType = type;
                        }

                        var result = TestType(testType);
                        foundBadTypes = foundBadTypes || !result;

                        DescribeTestOutcome(name, result);
                    }
                }
            }

            Assert.IsFalse(foundBadTypes, "Found one or more non-serializable, or not yet categorized types.");
        }

        #region RULES FOR IGNORING CERTAIN TYPES AS IRRELEVANT
        private static readonly HashSet<string> Assemblies = new HashSet<string>
        {
            "Microsoft.StreamProcessing",
        };

        private static readonly List<string> TypePartsToSkip = new List<string>
        {
            "Transformer",     // transformers can be skipped
            "Attribute",       // attributes can be skipped
            "StreamOperator",  // operators can be skipped
            "Window",          // window operators can be skipped
            "Comparer",        // comparers can be skipped
            "Exception",       // exceptions can be skipped
            "Plan",            // Plan related classes can be skipped
            "Policy",          // policy classes can be skipped
            "Pool",            // mem pools can be skipped
            "StreamScheduler", // scheduler can be skipped

            // Types manually looked at and skipped
            "ShardedStreamSerializer",
            "ShardedStreamCache",
            "FileObservable",
            "Collections.CharArray10",
            "Collections.CharArray100",
            "Collections.CharArray15",
            "Collections.CharArray16",
            "Collections.CharArray25",
            "Collections.CharArray255",
            "Collections.CharArray3",
            "Collections.CharArray32",
            "Collections.CharArray6",
            "Collections.CharArray64",
            "StreamMessage",
            "Microsoft.StreamProcessing.CharArray",
            "ShardedStreamable",
            "GeneratedAggregate",
            "DerivedProperties",
            "EvolvingStateEnumerable",
            "StreamProperties",
            "LocalLocationDescriptor",
            "LocalDestinationDescriptor",
            "Microsoft.StreamProcessing.IO.TextFileDataReader",
        };

        private static readonly List<Type> TypeHierarchiesToSkip = new List<Type>
        {
            typeof(Checkpointable), // Skip pipes
            typeof(Streamable<,>),  // Skip streamables
        };

        private static readonly HashSet<Type> TypesToSkip = new HashSet<Type>
        {
            typeof(QueryContainer),
            typeof(Process),
            typeof(ConfigModifier),
            typeof(Afa<,,>),
            typeof(Arc<,>),
            typeof(SingleElementArc<,>),
            typeof(ListElementArc<,>),
            typeof(MultiElementArc<,,>),
            typeof(EpsilonArc<,>),
            typeof(SingleEventArcInfo<,>),
            typeof(EventListArcInfo<,>),
            typeof(MultiEventArcInfo<,,>),
            typeof(GroupedActiveState<,>),
            typeof(PatternMatcher<,,,>),
        };
        #endregion END OF RULES

        private static bool SkipTypes(Type t)
        {
            // Skip all Enums, Interfaces, and Static classes.
            if (t.IsEnum ||
                t.IsInterface ||
                (t.IsAbstract && t.IsSealed)) // sealed-abstract class means static - go figure...
                return true;

            var name = t.FullName;
            if (TypePartsToSkip.Any(p => name.Contains(p)))
                return true;

            var tt = t;
            while (tt != null && tt != typeof(object))
            {
                if (TypeHierarchiesToSkip
                    .Any(baseType => baseType.IsGenericTypeDefinition
                                        ? tt.IsGenericType && tt.GetGenericTypeDefinition() == baseType
                                        : tt == baseType))
                    return true;
                tt = tt.BaseType;
            }

            // Note: for generic types compare with generic type definition.
            return TypesToSkip.Any(e => t.IsGenericType ? t.GetGenericTypeDefinition() == e : t == e);
        }

        private static string DescribeGeneric(string name, IEnumerable<Type> args)
        {
            if (name.Contains('`'))
                name = name.Substring(0, name.LastIndexOf('`'));
            return name + "<" + string.Join(", ", args.Select(a => DescribeType(a))) + ">";
        }

        private static string DescribeType(Type type)
        {
            if (type == null || type == typeof(void))
                return "void";
            if (type.IsGenericParameter)
                return type.Name;
            else if (type.IsGenericType)
                return DescribeGeneric(type.FullName, type.GetGenericArguments());
            else
                return type.FullName;
        }

        private static void DescribeTestOutcome(string name, bool result)
        {
            var savedColor = Console.ForegroundColor;
            Console.Write("\t==>[");
            Console.ForegroundColor = result ? ConsoleColor.Green : ConsoleColor.Red;
            Console.Write("{0}", result ? "PASS" : "FAIL");
            Console.ForegroundColor = savedColor;
            Console.Write("] Type: ");
            Console.ForegroundColor = result ? savedColor : ConsoleColor.White;
            Console.WriteLine("{0}", name);
            Console.ForegroundColor = savedColor;
        }

        private static bool TestType(Type t)
        {
            try
            {
                // Just check if type passes serialization checks.
                t.ValidateTypeForSerializer();
                return true;
            }
            catch (SerializationException)
            {
                return false;
            }
        }
    }
}
