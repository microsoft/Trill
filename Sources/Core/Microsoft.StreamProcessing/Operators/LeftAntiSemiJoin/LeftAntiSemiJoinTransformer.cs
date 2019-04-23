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
        private string genericParameters = string.Empty;
        private string BatchGeneratedFrom_TKey_TLeft;
        private string TKeyTLeftGenericParameters;
        private string TKeyTRightGenericParameters;
        private IEnumerable<MyFieldInfo> leftFields;
        private Func<string, string, string> keyComparer;
        private Func<string, string, string> leftComparer;
        private string ActiveEventType;
        private bool noLeftFields;

        private LeftAntiSemiJoinTemplate(string className, Type keyType, Type leftType, Type rightType)
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
        internal static Tuple<Type, string> Generate<TKey, TLeft, TRight>(LeftAntiSemiJoinStreamable<TKey, TLeft, TRight> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(BinaryPipe<TKey, TLeft, TRight, TLeft>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            var template = new LeftAntiSemiJoinTemplate($"GeneratedLeftAntiSemiJoin_{LASJSequenceNumber++}", typeof(TKey), typeof(TLeft), typeof(TRight));

            var gps = template.tm.GenericTypeVariables(template.keyType, template.leftType, template.rightType);
            template.genericParameters = gps.BracketedCommaSeparatedString();

            var leftMessageRepresentation = new ColumnarRepresentation(template.leftType);
            var resultRepresentation = new ColumnarRepresentation(template.leftType);

            #region Key Comparer
            var keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            template.keyComparer =
                (left, right) =>
                    keyComparer.Inline(left, right);
            #endregion

            #region Left Comparer
            template.ActiveEventType = template.leftType.GetTypeInfo().IsValueType ? template.TLeft : "Active_Event";
            template.noLeftFields = leftMessageRepresentation.noFields;

            var leftComparer = stream.LeftComparer.GetEqualsExpr();
            var newLambda = Extensions.TransformFunction<TKey, TLeft>(leftComparer, "index");
            template.leftComparer = (left, right) => newLambda.Inline(left, right);
            #endregion

            template.BatchGeneratedFrom_TKey_TLeft = Transformer.GetBatchClassName(template.keyType, template.leftType);
            template.TKeyTLeftGenericParameters = template.tm.GenericTypeVariables(template.keyType, template.leftType).BracketedCommaSeparatedString();

            template.TKeyTRightGenericParameters = template.tm.GenericTypeVariables(template.keyType, template.rightType).BracketedCommaSeparatedString();

            template.leftFields = resultRepresentation.AllFields;

            return template.Generate<TKey, TLeft, TRight>(leftComparer);
        }
    }
}
