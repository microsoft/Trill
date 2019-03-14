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
    internal partial class ClipJoinTemplate
    {
        private static int ClipSequenceNumber = 0;
        private string genericParameters = string.Empty;
        private string BatchGeneratedFrom_TKey_TLeft;
        private string TKeyTLeftGenericParameters;
        private string BatchGeneratedFrom_TKey_TRight;
        private string TKeyTRightGenericParameters;
        private IEnumerable<MyFieldInfo> leftFields;
        private Func<string, string, string> keyComparer;
        private Func<string, string, string> leftComparer;
        private string ActiveEventType;
        private bool noFields;

        private ClipJoinTemplate(string className, Type keyType, Type leftType, Type rightType)
            : base(className, keyType, leftType, rightType, leftType) { }

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
        internal static Tuple<Type, string> Generate<TKey, TLeft, TRight>(ClipJoinStreamable<TKey, TLeft, TRight> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(BinaryPipe<TKey, TLeft, TRight, TLeft>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            var template = new ClipJoinTemplate($"GeneratedClip_{ClipSequenceNumber++}", typeof(TKey), typeof(TLeft), typeof(TRight));

            var keyType = typeof(TKey);
            var leftType = typeof(TLeft);
            var rightType = typeof(TRight);

            var gps = template.tm.GenericTypeVariables(keyType, leftType, rightType);
            template.genericParameters = gps.BracketedCommaSeparatedString();

            var resultRepresentation = new ColumnarRepresentation(leftType);

            template.ActiveEventType = leftType.GetTypeInfo().IsValueType ? template.TLeft : "Active_Event";

            #region Key Comparer
            var keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            template.keyComparer =
                (left, right) =>
                    keyComparer.Inline(left, right);
            #endregion

            #region Left Comparer
            var leftComparer = stream.LeftComparer.GetEqualsExpr();
            var newLambda = Extensions.TransformFunction<TKey, TLeft>(leftComparer, "index_ProcessLeftEvent", 0);
            template.leftComparer = (left, right) => newLambda.Inline(left, right);
            #endregion

            template.BatchGeneratedFrom_TKey_TLeft = Transformer.GetBatchClassName(keyType, leftType);
            template.TKeyTLeftGenericParameters = template.tm.GenericTypeVariables(keyType, leftType).BracketedCommaSeparatedString();

            template.BatchGeneratedFrom_TKey_TRight = Transformer.GetBatchClassName(keyType, rightType);
            template.TKeyTRightGenericParameters = template.tm.GenericTypeVariables(keyType, rightType).BracketedCommaSeparatedString();

            template.leftFields = resultRepresentation.AllFields;
            template.noFields = resultRepresentation.noFields;

            return template.Generate<TKey, TLeft, TRight>(leftComparer);
        }
    }
}
