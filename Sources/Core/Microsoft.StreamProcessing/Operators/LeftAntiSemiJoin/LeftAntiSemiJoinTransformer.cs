// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class LeftAntiSemiJoinTemplate
    {
        private static int LASJSequenceNumber = 0;
        private Type keyType;
        private Type leftType;
        private Type rightType;
        private string TKey;
        private string TLeft;
        private string TRight;
        private string className;
        private string genericParameters = string.Empty;
        private string BatchGeneratedFrom_TKey_TLeft;
        private string TKeyTLeftGenericParameters;
        private string BatchGeneratedFrom_TKey_TRight;
        private string TKeyTRightGenericParameters;
        private IEnumerable<MyFieldInfo> leftFields;
        private Func<string, string, string> keyComparer;
        private Func<string, string, string> leftComparer;
        private string staticCtor;
        private string ActiveEventType;
        private bool noLeftFields;
        private bool leftIsConstantDuration;

        private LeftAntiSemiJoinTemplate() { }

        /// <summary>
        /// Generate a batch class definition to be used as a Clip pipe.
        /// Compile the definition, dynamically load the assembly containing it, and return the Type representing the
        /// aggregate class.
        /// </summary>
        /// <typeparam name="TKey">The key type for both sides.</typeparam>
        /// <typeparam name="TLeft">The payload type for the left side.</typeparam>
        /// <typeparam name="TRight">The payload type for the right side.</typeparam>
        /// <returns>
        /// A type that is defined to be a subtype of BinaryPipe&lt;<typeparamref name="TKey"/>,<typeparamref name="TLeft"/>, <typeparamref name="TKey"/>, <typeparamref name="TRight"/>&gt;.
        /// </returns>
        internal static Tuple<Type, string> Generate<TKey, TLeft, TRight>(LeftAntiSemiJoinStreamable<TKey, TLeft, TRight> stream, bool leftIsConstantDuration)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(BinaryPipe<TKey, TLeft, TRight, TLeft>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string errorMessages = null;
            try
            {
                var template = new LeftAntiSemiJoinTemplate();

                var keyType = template.keyType = typeof(TKey);
                var leftType = template.leftType = typeof(TLeft);
                var rightType = template.rightType = typeof(TRight);

                var tm = new TypeMapper(keyType, leftType, rightType);
                template.TKey = tm.CSharpNameFor(keyType);
                template.TLeft = tm.CSharpNameFor(leftType);
                template.TRight = tm.CSharpNameFor(rightType);
                var gps = tm.GenericTypeVariables(keyType, leftType, rightType);
                template.genericParameters = gps.BracketedCommaSeparatedString();

                template.className = $"GeneratedLeftAntiSemiJoin_{LASJSequenceNumber++}";

                var leftMessageRepresentation = new ColumnarRepresentation(leftType);
                var resultRepresentation = new ColumnarRepresentation(leftType);

                #region Key Comparer
                var keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
                template.keyComparer =
                    (left, right) =>
                        keyComparer.Inline(left, right);
                #endregion

                #region Left Comparer
                template.ActiveEventType = leftType.GetTypeInfo().IsValueType ? template.TLeft : "Active_Event";
                template.noLeftFields = leftMessageRepresentation.noFields;
                template.leftIsConstantDuration = leftIsConstantDuration;

                var leftComparer = stream.LeftComparer.GetEqualsExpr();
                var newLambda = Extensions.TransformFunction<TKey, TLeft>(leftComparer, "index");
                template.leftComparer = (left, right) => newLambda.Inline(left, right);
                #endregion

                template.BatchGeneratedFrom_TKey_TLeft = Transformer.GetBatchClassName(keyType, leftType);
                template.TKeyTLeftGenericParameters = tm.GenericTypeVariables(keyType, leftType).BracketedCommaSeparatedString();

                template.BatchGeneratedFrom_TKey_TRight = Transformer.GetBatchClassName(keyType, rightType);
                template.TKeyTRightGenericParameters = tm.GenericTypeVariables(keyType, rightType).BracketedCommaSeparatedString();

                template.leftFields = resultRepresentation.AllFields;

                template.staticCtor = Transformer.StaticCtor(template.className);
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(typeof(TKey), typeof(TLeft), typeof(TRight));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TLeft>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TRight>());
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(leftComparer));

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var realClassName = template.className.AddNumberOfNecessaryGenericArguments(keyType, leftType, rightType);
                var t = a.GetType(realClassName);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = keyType.GetAnonymousTypes();
                    list.AddRange(leftType.GetAnonymousTypes());
                    list.AddRange(rightType.GetAnonymousTypes());
                    return Tuple.Create(t.MakeGenericType(list.ToArray()), errorMessages);
                }
                else return Tuple.Create(t, errorMessages);
            }
            catch
            {
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                {
                    throw new InvalidOperationException("Code Generation failed when it wasn't supposed to!");
                }
                return Tuple.Create((Type)null, errorMessages);
            }
        }

    }
}
