// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class MultiStringTransformer
    {
        public struct MultiStringTransformationResult
        {
            public Expression transformedExpression;
            public Dictionary<FieldInfo, ParameterExpression> wrapperTable;
            public string vectorOperation;

        }

        public static MultiStringTransformationResult Transform(Type t, Expression e)
        {
            Contract.Requires(Config.MultiStringTransforms != Config.CodegenOptions.MultiStringFlags.None);

            if (((Config.MultiStringTransforms & Config.CodegenOptions.MultiStringFlags.VectorOperations) != 0) &&
                IsVectorizable(t, e))
            {
                var vectorStatements = Vectorize.Transform(t, e);
                return new MultiStringTransformationResult
                {
                    vectorOperation = string.Join("\n", vectorStatements),
                    wrapperTable = new Dictionary<FieldInfo, ParameterExpression>(),
                };
            }
            var wrapperVisitor = new WrapperTransformer(t);
            var res = wrapperVisitor.Visit(e);
            return new MultiStringTransformationResult
            {
                transformedExpression = res,
                wrapperTable = wrapperVisitor.multiStringTable
            };
        }

        private static bool IsVectorizable(Type batchType, Expression e)
        {
            // For now, the only vectorizable things are boolean combinatios of
            // method calls to certain String methods where none of the arguments
            // to the call contain any calls to any String methods.
            // E.g., s.Contains(t) is allowed, s.Contains(t.Substring(...)) is not.
            // s.Equals(t) || s.Contains(u) is allowed
            if (!(e is BinaryExpression || e is UnaryExpression || e is MethodCallExpression || e is MemberExpression))
                return false;

            var vectorVisitor = new VectorizableVisitor(batchType);
            vectorVisitor.Visit(e);
            return vectorVisitor.IsVectorizable;
        }

        private sealed class VectorizableVisitor : ExpressionVisitor
        {
            public bool IsVectorizable = true;
            private readonly Type batchType;

            public VectorizableVisitor(Type batchType) => this.batchType = batchType;

            public static bool IsMemberOnBatchField(MemberExpression memberBinding, Type batchType)
                => memberBinding == null
                    ? false
                    : memberBinding.Member.DeclaringType == batchType;

            protected override Expression VisitBinary(BinaryExpression node)
            {
                switch (node.NodeType)
                {
                    case ExpressionType.AndAlso:
                    case ExpressionType.OrElse:
                        Visit(node.Left);
                        Visit(node.Right);
                        return node;
                    default:
                        this.IsVectorizable = false;
                        return node;
                }
            }

            protected override Expression VisitUnary(UnaryExpression node)
            {
                switch (node.NodeType)
                {
                    case ExpressionType.Not:
                        Visit(node.Operand);
                        return node;
                    default:
                        this.IsVectorizable = false;
                        return node;
                }
            }

            protected override Expression VisitMethodCall(MethodCallExpression methodCall)
            {
                var calledMethod = methodCall.Method;

                // static bool System.Text.RegularExpressions.Regex.IsMatch(string input, string pattern)
                if (IsStaticRegexMatch(methodCall, this.batchType))
                    return methodCall;

                if (!(calledMethod.DeclaringType.Equals(typeof(string)) &&
                      IsMemberOnBatchField(methodCall.Object as MemberExpression, this.batchType) &&
                      MultiStringHasVectorImplementation(calledMethod.Name) &&
                      IsMemberOnBatchField(methodCall.Object as MemberExpression, this.batchType) &&
                      methodCall.Arguments.Any(a => !SearchForMultiStringMethod.ContainsMultiStringCall(this.batchType, a))))
                {
                    this.IsVectorizable = false;
                }
                return methodCall;

            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (!(node.Member.DeclaringType.Equals(typeof(string)) &&
                      IsMemberOnBatchField(node.Expression as MemberExpression, this.batchType) &&
                      MultiStringHasVectorImplementation(node.Member.Name)))
                {
                    this.IsVectorizable = false;
                }
                return node;
            }

            public static bool IsStaticRegexMatch(MethodCallExpression methodCall, Type batchType)
            {
                var calledMethod = methodCall.Method;

                // static bool System.Text.RegularExpressions.Regex.IsMatch(string input, string pattern)
                return calledMethod.DeclaringType.Equals(typeof(System.Text.RegularExpressions.Regex)) &&
                    calledMethod.IsStatic &&
                    calledMethod.Name.Equals("IsMatch") &&
                    methodCall.Arguments.Count == 2 &&
                    methodCall.Arguments.All(a => a.Type.Equals(typeof(string))) &&
                    methodCall.Arguments.All(a => !SearchForMultiStringMethod.ContainsMultiStringCall(batchType, a)) &&
                    IsMemberOnBatchField(methodCall.Arguments.ElementAt(0) as MemberExpression, batchType);
            }
        }

        private sealed class Vectorize : ExpressionVisitor
        {
            private static int counter = 0;
            public List<string> vectorStatements = new List<string>();
            private string incomingBV = "batch.bitvector";
            private bool inPlace = false;
            private string resultBV;
            private readonly Type batchType;

            private Vectorize(Type batchType) => this.batchType = batchType;

            public static IEnumerable<string> Transform(Type batchType, Expression e)
            {
                var me = new Vectorize(batchType);
                me.Visit(e);
                if (!string.IsNullOrWhiteSpace(me.resultBV))
                {
                    me.vectorStatements.Add("var tmp = batch.bitvector;");
                    me.vectorStatements.Add("batch.bitvector = " + me.resultBV + ";");
                    me.vectorStatements.Add("tmp.ReturnClear();");
                }
                return me.vectorStatements;
            }

            protected override Expression VisitBinary(BinaryExpression node)
            {
                string left_result;
                switch (node.NodeType)
                {
                    case ExpressionType.OrElse:
                        this.inPlace = false;
                        Visit(node.Left);
                        left_result = this.resultBV;
                        var bv_i = "bv" + counter++;

                        // var bv_i = invert(left_result) | incomingBV;
                        this.vectorStatements.Add($"var {bv_i} = MultiString.InvertLeftThenOrWithRight({left_result}, {this.incomingBV}, this.pool.bitvectorPool);");
                        this.incomingBV = bv_i;
                        Visit(node.Right);

                        // free bv_i
                        this.vectorStatements.Add($"{bv_i}.ReturnClear();");

                        // this.resultBV &= left_result;
                        this.vectorStatements.Add($"MultiString.AndEquals({this.resultBV}, {left_result});");

                        // free left_result
                        this.vectorStatements.Add($"{left_result}.ReturnClear();");

                        break;
                    case ExpressionType.AndAlso:
                        Visit(node.Left);
                        left_result = this.resultBV;
                        this.incomingBV = this.resultBV;
                        Visit(node.Right);

                        // free left_result
                        this.vectorStatements.Add($"{left_result}.ReturnClear();");

                        break;
                    default:
                        Contract.Assume(false, "case meant to be exhaustive");
                        break;
                }
                return node;

            }

            /// <summary>
            /// Transforms e.f.M(...) into var vec_i = f_col.M(..., current_bit_vector, true)
            /// </summary>
            /// <param name="node"></param>
            /// <returns></returns>
            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1305:SpecifyIFormatProvider",
                MessageId = "System.string.Format(System.String,System.Object)",
                Justification = "This is CLR API substitution, additional intent should not be added on behalf of the user of the API."),
            System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1305:SpecifyIFormatProvider",
                MessageId = "System.string.Format(System.String,System.Object[])",
                Justification = "This is CLR API substitution, additional intent should not be added on behalf of the user of the API.")]
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                // static bool System.Text.RegularExpressions.Regex.IsMatch(string e.f, string pattern)
                // ==>
                // public unsafe ColumnBatch<long> IsMatch(string regex, ColumnBatch<long> inBV, bool inPlace = true)
                string methodCall;
                if (VectorizableVisitor.IsStaticRegexMatch(node, this.batchType))
                {
                    var args = string.Join(",", node.Arguments.Skip(1).Select(a => a.ExpressionToCSharp()));
                    methodCall = string.Format(
                        "{0}{1}_col.IsMatch({2}, {3}, {4});",
                        Transformer.ColumnFieldPrefix,
                        (node.Arguments.ElementAt(0) as MemberExpression).Member.Name,
                        args, this.incomingBV, this.inPlace ? "true" : "false");
                }
                else
                {
                    // assume that the method call is an instance method and that the receiver is
                    // the field being represented as a MultiString.
                    var memberExpression = node.Object as MemberExpression;
                    var member = memberExpression.Member;
                    var args = string.Join(",", node.Arguments.Select(a => a.ExpressionToCSharp()));
                    methodCall = string.Format(
                        "{0}{1}_col.{2}({3}, {4}, {5});",
                        Transformer.ColumnFieldPrefix,
                        member.Name,
                        node.Method.Name,
                        args, this.incomingBV, this.inPlace ? "true" : "false");
                }
                var s = string.Empty;
                if (this.inPlace)
                {
                    this.resultBV = this.incomingBV;
                }
                else
                {
                    this.resultBV = $"bv{counter++}";
                    s = "var " + this.resultBV + " = ";
                }
                s += methodCall;
                this.vectorStatements.Add(s);
                return node;
            }
        }

        private class SearchForMultiStringMethod : ExpressionVisitor
        {
            private bool found = false;
            private readonly Type batchType;

            private SearchForMultiStringMethod(Type t) => this.batchType = t;

            public static bool ContainsMultiStringCall(Type batchType, Expression e)
            {
                var me = new SearchForMultiStringMethod(batchType);
                me.Visit(e);
                return me.found;
            }

            private bool IsCallToStringMethodOnBatchField(MemberExpression memberBinding)
            {
                if (memberBinding == null) return false;
                var field = memberBinding.Member as FieldInfo;
                return field == null
                    ? false
                    : field.DeclaringType == this.batchType;
            }

            protected override Expression VisitMethodCall(MethodCallExpression methodCall)
            {
                if (methodCall.Method.DeclaringType != typeof(string)) goto JustVisit;
                if (!IsCallToStringMethodOnBatchField(methodCall.Object as MemberExpression)) goto JustVisit;

                this.found = true;
                return methodCall;

            JustVisit:
                return base.VisitMethodCall(methodCall);
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (!node.Member.DeclaringType.Equals(typeof(string))) goto JustVisit;
                if (!IsCallToStringMethodOnBatchField(node.Expression as MemberExpression)) goto JustVisit;

                this.found = true;
                return node;

            JustVisit:
                return base.VisitMember(node);
            }
        }

        /// <summary>
        /// Returns true iff MultiString has a vector implementation that returns
        /// a bit vector that represents occupancy information.
        /// </summary>
        /// <param name="methodName"></param>
        /// <returns></returns>
        private static bool MultiStringHasVectorImplementation(string methodName)
        {
            switch (methodName)
            {
                case "Contains":
                case "Equals":
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Replaces certain calls to methods on the String class into equivalent calls
        /// from the MultiString.MultiStringWrapper class by replacing the receiver
        /// of the method call with a variable of type MultiString.MultiStringWrapper.
        /// That variable is kept in a table so clients of this class can generate
        /// intializations for them in code before the transformed expression is executed.
        /// </summary>
        private class WrapperTransformer : ExpressionVisitor
        {
            private readonly Type batchType;
            public Dictionary<FieldInfo, ParameterExpression> multiStringTable = new Dictionary<FieldInfo, ParameterExpression>();

            public WrapperTransformer(Type t) => this.batchType = t;

            /// <summary>
            /// Translate calls to method calls on multistrings into calls to MultiString wrapper methods
            /// </summary>
            /// <param name="node"></param>
            /// <returns></returns>
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.DeclaringType != typeof(string)) goto JustVisit;
                if (!(MultiStringHasWrapperImplementation(node.Method.Name))) goto JustVisit;
                if (!(node.Object is MemberExpression memberBinding)) goto JustVisit;
                var field = memberBinding.Member as FieldInfo;
                if (field == null) goto JustVisit;
                if (field.DeclaringType != this.batchType) goto JustVisit;

                // also need to make sure that the instance is the parameter of the lambda, i.e. "e.f"
                // if it is e.f.g then g doesn't correspond to a multistring column on the batch
                var method = node.Method;

                if (!this.multiStringTable.TryGetValue(field, out ParameterExpression wrapper))
                {
                    var t = typeof(MultiString.MultiStringWrapper);
                    wrapper = Expression.Variable(t, field.Name + "_wrapper");
                    this.multiStringTable.Add(field, wrapper);
                }

                var wrapperMethod = typeof(MultiString.MultiStringWrapper).GetTypeInfo().GetMethod(method.Name, node.Arguments.Select(a => a.Type).ToArray());
                return Expression.Call(wrapper, wrapperMethod, node.Arguments);

            JustVisit:
                return base.VisitMethodCall(node);
            }

            /// <summary>
            /// Translate calls to property getters on multistrings into calls to MultiString wrapper methods
            /// </summary>
            /// <param name="node"></param>
            /// <returns></returns>
            protected override Expression VisitMember(MemberExpression node)
            {
                if (!node.Member.DeclaringType.Equals(typeof(string))) goto JustVisit;
                if (!MultiStringHasWrapperImplementation(node.Member.Name)) goto JustVisit;
                if (!(node.Expression is MemberExpression memberBinding)) goto JustVisit;
                var field = memberBinding.Member as FieldInfo;
                if (field == null) goto JustVisit;
                if (field.DeclaringType != this.batchType) goto JustVisit;

                if (!this.multiStringTable.TryGetValue(field, out ParameterExpression wrapper))
                {
                    var t = typeof(MultiString.MultiStringWrapper);
                    wrapper = Expression.Variable(t, field.Name + "_wrapper");
                    this.multiStringTable.Add(field, wrapper);
                }

                var wrapperMembers = typeof(MultiString.MultiStringWrapper).GetTypeInfo().GetMember(node.Member.Name);
                Contract.Assume(wrapperMembers.Length > 0);
                var wrapperMember = wrapperMembers[0];
                return Expression.MakeMemberAccess(wrapper, wrapperMember);

            JustVisit:
                return base.VisitMember(node);
            }

            private static bool MultiStringHasWrapperImplementation(string methodName)
            {
                switch (methodName)
                {
                    case "IndexOf":
                    case "LastIndexOf":
                    case "Length":
                        return true;
                    default:
                        return false;
                }
            }
        }
    }
}