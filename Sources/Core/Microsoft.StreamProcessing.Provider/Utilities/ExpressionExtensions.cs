// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal static class ExpressionExtensions
    {
        public static bool ExpressionEquals(this Expression source, Expression other)
            => EqualityComparer.IsEqual(source, other);
    }

    internal sealed class EqualityComparer : ExpressionVisitor
    {
        private readonly Dictionary<ParameterExpression, int> parameterMap = new Dictionary<ParameterExpression, int>();
        private int uniqueParameterNumber;

        private EqualityComparer() => this.uniqueParameterNumber = 0;

        public static bool IsEqual(Expression e1, Expression e2)
        {
            var me = new EqualityComparer();
            return me.Equals(e1, e2);
        }

        private bool Equals(Expression e1, Expression e2)
        {
            if (e1 == e2) return true;
            if (e1 == null || e2 == null) return false;
            if (e1.NodeType != e2.NodeType) return false;
            if (e1 is BinaryExpression b1)
            {
                var b2 = e2 as BinaryExpression;
                return Equals(b1.Left, b2.Left) && Equals(b1.Right, b2.Right);
            }
            if (e1 is UnaryExpression u1)
            {
                var u2 = e2 as UnaryExpression;
                return Equals(u1.Operand, u2.Operand);
            }
            if (e1 is ConditionalExpression conditional1)
            {
                var conditional2 = e2 as ConditionalExpression;
                return Equals(conditional1.Test, conditional2.Test) && Equals(conditional1.IfTrue, conditional2.IfTrue) && Equals(conditional1.IfFalse, conditional2.IfFalse);
            }
            if (e1 is ConstantExpression constant1)
            {
                var constant2 = e2 as ConstantExpression;
                return constant1.Value == null ? constant2.Value == null : constant1.Value.Equals(constant2.Value);
            }
            if (e1 is ParameterExpression param1)
            {
                var param2 = e2 as ParameterExpression;
                return this.parameterMap[param1] == this.parameterMap[param2];
            }
            if (e1 is IndexExpression index1)
            {
                var index2 = e2 as IndexExpression;
                return Equals(index1.Object, index2.Object) && Equals(index1.Arguments, index2.Arguments);
            }
            if (e1 is InvocationExpression invoke1)
            {
                var invoke2 = e2 as InvocationExpression;
                return Equals(invoke1.Expression, invoke2.Expression) && Equals(invoke1.Arguments, invoke2.Arguments);
            }
            if (e1 is LambdaExpression lambda1)
            {
                var lambda2 = e2 as LambdaExpression;
                if (!lambda1.ReturnType.Equals(lambda2.ReturnType)) return false;
                if (lambda1.Parameters.Count != lambda2.Parameters.Count) return false;
                for (int i = 0; i < lambda1.Parameters.Count; i++)
                {
                    var v1 = lambda1.Parameters[i];
                    var v2 = lambda2.Parameters[i];
                    this.parameterMap.Add(v1, this.uniqueParameterNumber);
                    if (v1 != v2) this.parameterMap.Add(v2, this.uniqueParameterNumber);
                    this.uniqueParameterNumber++;
                }
                var result = Equals(lambda1.Body, lambda2.Body);
                for (int i = 0; i < lambda1.Parameters.Count; i++)
                {
                    this.parameterMap.Remove(lambda1.Parameters[i]);
                    this.parameterMap.Remove(lambda2.Parameters[i]);
                }
                return result;
            }
            if (e1 is MemberExpression member1)
            {
                var member2 = e2 as MemberExpression;
                return member1.Member.Equals(member2.Member) && Equals(member1.Expression, member2.Expression);
            }
            if (e1 is MethodCallExpression mc1)
            {
                var mc2 = e2 as MethodCallExpression;
                return mc1.Method.Equals(mc2.Method) && Equals(mc1.Arguments, mc2.Arguments);
            }
            if (e1 is NewExpression new1)
            {
                var new2 = e2 as NewExpression;
                return new1.Constructor == null
                    ? new2.Constructor == null
                    : new1.Constructor.Equals(new2.Constructor) && Equals(new1.Arguments, new2.Arguments);
            }
            if (e1 is NewArrayExpression newarr1)
            {
                var newarr2 = e2 as NewArrayExpression;
                return newarr1.Type.Equals(newarr2.Type) && Equals(newarr1.Expressions, newarr2.Expressions);
            }
            if (e1 is MemberInitExpression memInit1)
            {
                var memInit2 = e2 as MemberInitExpression;
                return Equals(memInit1.NewExpression, memInit2.NewExpression) && memInit1.Bindings.Count == memInit2.Bindings.Count
                    && memInit1.Bindings.Zip(memInit2.Bindings, (a, b) => Tuple.Create(a, b)).All(r => Equals(r.Item1, r.Item2));
            }
            if (e1 is BlockExpression block1)
            {
                var block2 = e2 as BlockExpression;
                return Equals(block1.Expressions, block2.Expressions);
            }
            if (e1 is LoopExpression loop1)
            {
                var loop2 = e2 as LoopExpression;
                return Equals(loop1.Body, loop2.Body)
                    && loop1.BreakLabel.Name == loop2.BreakLabel.Name
                    && loop1.ContinueLabel.Name == loop2.ContinueLabel.Name;
            }
            if (e1 is LabelExpression label1)
            {
                var label2 = e2 as LabelExpression;
                return Equals(label1.DefaultValue, label2.DefaultValue) && label1.Target.Name == label2.Target.Name;
            }
            return false;
        }

        private bool Equals(ReadOnlyCollection<Expression> list1, ReadOnlyCollection<Expression> list2)
        {
            if (list1.Count != list2.Count) return false;
            for (int i = 0; i < list1.Count; i++)
            {
                if (!Equals(list1[i], list2[i])) return false;
            }
            return true;
        }

        private bool Equals(MemberBinding mb1, MemberBinding mb2)
        {
            if (mb1.Member != mb2.Member) return false;
            switch (mb1.BindingType)
            {
                case MemberBindingType.Assignment:
                    {
                        if (mb2.BindingType != MemberBindingType.Assignment) return false;
                        var a1 = mb1 as MemberAssignment;
                        var a2 = mb2 as MemberAssignment;
                        return Equals(a1.Expression, a2.Expression);
                    }
                case MemberBindingType.ListBinding:
                    {
                        if (mb2.BindingType != MemberBindingType.ListBinding) return false;
                        var l1 = mb1 as MemberListBinding;
                        var l2 = mb2 as MemberListBinding;
                        return l1.Initializers.Count == l2.Initializers.Count
                            && Enumerable.Range(0, l1.Initializers.Count).All(o => l1.Initializers[o].AddMethod == l2.Initializers[o].AddMethod && Equals(l1.Initializers[o].Arguments, l2.Initializers[o].Arguments));
                    }
                case MemberBindingType.MemberBinding:
                    {
                        if (mb2.BindingType != MemberBindingType.MemberBinding) return false;
                        var v1 = mb1 as MemberMemberBinding;
                        var v2 = mb2 as MemberMemberBinding;
                        return v1.Bindings.Count == v2.Bindings.Count
                            && Enumerable.Range(0, v1.Bindings.Count).All(o => Equals(v1.Bindings[o], v2.Bindings[o]));
                    }
                default: throw new InvalidOperationException("Switch statement meant to be exhaustive.");
            }
        }
    }
}
