// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal abstract class CommonBinaryTemplate : CommonPipeTemplate
    {
        protected readonly Type keyType;
        protected readonly Type leftType;
        protected readonly Type rightType;
        protected readonly Type resultType;

        protected readonly string TKey;
        protected readonly string TLeft;
        protected readonly string TRight;
        protected readonly string TResult;

        protected readonly TypeMapper tm;

        protected CommonBinaryTemplate(string className, Type keyType, Type leftType, Type rightType, Type resultType)
            : base(className)
        {
            this.keyType = keyType;
            this.leftType = leftType;
            this.rightType = rightType;
            this.resultType = resultType;

            this.tm = new TypeMapper(keyType, leftType, rightType, resultType);
            this.TKey = this.tm.CSharpNameFor(keyType);
            this.TLeft = this.tm.CSharpNameFor(leftType);
            this.TRight = this.tm.CSharpNameFor(rightType);
            this.TResult = this.tm.CSharpNameFor(resultType);
        }

        protected Tuple<Type, string> Generate<TKey, TPayload>() => GenerateInternal<TKey, TPayload, TPayload, TPayload>(2, null);

        protected Tuple<Type, string> Generate<TKey, TLeft, TRight>(Expression expression = null)
            => GenerateInternal<TKey, TLeft, TRight, TLeft>(3, expression);

        protected Tuple<Type, string> Generate<TKey, TLeft, TRight, TResult>(Expression expression = null)
            => GenerateInternal<TKey, TLeft, TRight, TResult>(4, expression);

        private Tuple<Type, string> GenerateInternal<TKey, TLeft, TRight, TResult>(int numParameters, Expression expression)
        {
            string errorMessages = null;
            try
            {
                var expandedCode = TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(this.keyType, this.leftType, this.rightType, this.resultType);
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TLeft>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TRight>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TResult>());
                if (expression != null) assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(expression));

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                if (this.keyType.IsAnonymousType())
                {
                    if (errorMessages == null) errorMessages = string.Empty;
                    errorMessages += "\nCodegen Warning: The key type for a binary operator is an anonymous type (or contains an anonymous type), preventing the inlining of the key equality and hashcode functions. This may lead to poor performance.\n";
                }

                var types = new List<Type> { this.keyType, this.leftType };
                if (numParameters > 2) types.Add(this.rightType);
                if (numParameters == 4) types.Add(this.resultType);
                var realClassName = this.className.AddNumberOfNecessaryGenericArguments(types.ToArray());
                var t = a.GetType(realClassName);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = this.keyType.GetAnonymousTypes();
                    list.AddRange(this.leftType.GetAnonymousTypes());
                    if (numParameters > 2) list.AddRange(this.rightType.GetAnonymousTypes());
                    if (numParameters == 4) list.AddRange(this.resultType.GetAnonymousTypes());
                    t = t.MakeGenericType(list.ToArray());
                }
                return Tuple.Create(t, errorMessages);
            }
            catch
            {
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                    throw new InvalidOperationException("Code Generation failed when it wasn't supposed to!");

                return Tuple.Create((Type)null, errorMessages);
            }
        }
    }
}