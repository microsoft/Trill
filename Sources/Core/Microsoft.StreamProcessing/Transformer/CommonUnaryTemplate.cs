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
    internal abstract class CommonUnaryTemplate : CommonPipeTemplate
    {
        protected readonly Type keyType;
        protected readonly Type payloadType;
        protected readonly Type resultType;

        protected readonly string TKey;
        protected readonly string TPayload;
        protected readonly string TResult;

        protected string BatchGeneratedFrom_TKey_TPayload;
        protected string TKeyTPayloadGenericParameters;

        protected ColumnarRepresentation payloadRepresentation;
        protected ColumnarRepresentation resultRepresentation;

        protected IEnumerable<MyFieldInfo> fields;
        protected readonly bool noFields;

        protected CommonUnaryTemplate(string className, Type keyType, Type payloadType, Type resultType, bool suppressPayloadBatch = false)
            : base(className)
        {
            this.keyType = keyType;
            this.payloadType = payloadType;
            this.resultType = resultType;

            var tm = new TypeMapper(keyType, payloadType, resultType);
            this.TKey = tm.CSharpNameFor(keyType);
            this.TPayload = tm.CSharpNameFor(payloadType);
            this.TResult = tm.CSharpNameFor(resultType);

            if (!suppressPayloadBatch)
            {
                this.TKeyTPayloadGenericParameters = tm.GenericTypeVariables(keyType, payloadType).BracketedCommaSeparatedString();
                this.BatchGeneratedFrom_TKey_TPayload = Transformer.GetBatchClassName(keyType, payloadType);
            }

            this.payloadRepresentation = new ColumnarRepresentation(payloadType);
            this.fields = this.payloadRepresentation.AllFields;
            this.noFields = this.payloadRepresentation.noFields;

            this.resultRepresentation = new ColumnarRepresentation(resultType);
        }

        protected Tuple<Type, string> Generate<TKey, TPayload>(params Type[] types)
        {
            string errorMessages = null;
            try
            {
                var expandedCode = TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(this.keyType, this.payloadType, typeof(SortedDictionary<,>));
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(types));
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var realClassName = this.className.AddNumberOfNecessaryGenericArguments(this.keyType, this.payloadType);
                var t = a.GetType(realClassName);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = this.keyType.GetAnonymousTypes();
                    list.AddRange(this.payloadType.GetAnonymousTypes());
                    return Tuple.Create(t.MakeGenericType(list.ToArray()), errorMessages);
                }
                else return Tuple.Create(t, errorMessages);
            }
            catch (Exception e)
            {
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                    throw new InvalidOperationException("Code Generation failed when it wasn't supposed to!", e);

                return Tuple.Create((Type)null, errorMessages);
            }
        }

        protected Tuple<Type, string> Generate<TKey, TPayload, TResult>(Type[] types, Expression[] expressions)
        {
            string errorMessages = null;
            try
            {
                var expandedCode = TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(this.keyType, this.payloadType, this.resultType, typeof(SortedDictionary<,>));
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(types));
                if (expressions != null)
                {
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(expressions));
                }

                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TResult>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var realClassName = this.className.AddNumberOfNecessaryGenericArguments(this.keyType, this.payloadType);
                var t = a.GetType(realClassName);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = this.keyType.GetAnonymousTypes();
                    list.AddRange(this.payloadType.GetAnonymousTypes());
                    return Tuple.Create(t.MakeGenericType(list.ToArray()), errorMessages);
                }
                else return Tuple.Create(t, errorMessages);
            }
            catch (Exception e)
            {
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                    throw new InvalidOperationException("Code Generation failed when it wasn't supposed to!", e);

                return Tuple.Create((Type)null, errorMessages);
            }
        }
    }
}