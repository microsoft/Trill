// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal static class PlaceholderMethod
    {
        public static string Text = "Microsoft.StreamProcessing.PlaceholderMethod.Foo()";
        public static void Foo() => throw new InvalidOperationException();
    }

    internal sealed class FuseModule
    {
        private readonly List<ExpressionProfile> expressions = new List<ExpressionProfile>();
        private Expression durationAdjustment = null;

        public FuseModule() { }

        private FuseModule(FuseModule that)
        {
            this.expressions = new List<ExpressionProfile>(that.expressions.Select(o => o.Clone()));
            this.durationAdjustment = that.durationAdjustment;
        }

        public bool Any => this.expressions.Count > 0;

        public FuseModule Clone() => new FuseModule(this);

        public Expression[] GetCodeGenExpressions() => this.expressions.Select(profile => (Expression)profile.expression).ToArray();

        public FuseModule FuseSelect<TPayload, TResult>(Expression<Func<TPayload, TResult>> selector)
        {
            if (this.expressions.Count == 0)
            {
                this.expressions.Add(
                    new ExpressionProfile
                    {
                        category = ExpressionCategory.Select,
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                return this;
            }
            var prev = this.expressions[this.expressions.Count - 1];
            switch (prev.category)
            {
                case ExpressionCategory.Select:
                {
                    var body = selector.ReplaceParametersInBody(prev.expression.Body);

                    prev.outputType = typeof(TResult);
                    prev.expression = Expression.Lambda(body, prev.expression.Parameters);
                    break;
                }

                default:
                {
                    this.expressions.Add(new ExpressionProfile
                    {
                        category = ExpressionCategory.Select,
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                    break;
                }
            }
            return this;
        }

        public FuseModule FuseSelect<TPayload, TResult>(Expression<Func<long, TPayload, TResult>> selector)
        {
            if (this.expressions.Count == 0)
            {
                this.expressions.Add(
                    new ExpressionProfile
                    {
                        category = ExpressionCategory.Select,
                        hasStartEdge = true,
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                return this;
            }
            var prev = this.expressions[this.expressions.Count - 1];
            switch (prev.category)
            {
                case ExpressionCategory.Select:
                {
                    IEnumerable<ParameterExpression> parameters = prev.expression.Parameters;
                    var body = ParameterSubstituter.Replace(selector.Parameters[1], prev.expression.Body, selector.Body);

                    if (prev.category == ExpressionCategory.Select && prev.hasStartEdge) body = ParameterSubstituter.Replace(selector.Parameters[0], prev.expression.Parameters[0], body);
                    else parameters = selector.Parameters[0].Yield().Concat(parameters);

                    prev.outputType = typeof(TResult);
                    prev.expression = Expression.Lambda(body, parameters);
                    break;
                }

                default:
                {
                    this.expressions.Add(new ExpressionProfile
                    {
                        category = ExpressionCategory.Select,
                        hasStartEdge = true,
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                    break;
                }
            }
            return this;
        }

        public FuseModule FuseSelectWithKey<TKey, TPayload, TResult>(Expression<Func<TKey, TPayload, TResult>> selector)
        {
            if (this.expressions.Count == 0)
            {
                this.expressions.Add(
                    new ExpressionProfile
                    {
                        category = ExpressionCategory.Select,
                        hasKey = true,
                        keyType = typeof(TKey),
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                return this;
            }
            var prev = this.expressions[this.expressions.Count - 1];
            switch (prev.category)
            {
                case ExpressionCategory.Select:
                {
                    var body = ParameterSubstituter.Replace(selector.Parameters[1], prev.expression.Body, selector.Body);
                    var payloadParameter = prev.expression.Parameters.Last().Yield();

                    var startEdgeParameter = prev.hasStartEdge
                        ? prev.expression.Parameters[0].Yield()
                        : Enumerable.Empty<ParameterExpression>();

                    IEnumerable<ParameterExpression> keyParameter;
                    if (prev.hasKey)
                    {
                        var key = prev.expression.Parameters[prev.expression.Parameters.Count - 2];
                        body = ParameterSubstituter.Replace(selector.Parameters[0], key, body);
                        keyParameter = key.Yield();
                    }
                    else keyParameter = selector.Parameters[0].Yield();

                    prev.outputType = typeof(TResult);
                    prev.hasKey = true;
                    prev.keyType = typeof(TKey);
                    prev.expression = Expression.Lambda(body, startEdgeParameter.Concat(keyParameter).Concat(payloadParameter));
                    break;
                }

                default:
                {
                    this.expressions.Add(new ExpressionProfile
                    {
                        category = ExpressionCategory.Select,
                        hasKey = true,
                        keyType = typeof(TKey),
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                    break;
                }
            }
            return this;
        }

        public FuseModule FuseSelectWithKey<TKey, TPayload, TResult>(Expression<Func<long, TKey, TPayload, TResult>> selector)
        {
            if (this.expressions.Count == 0)
            {
                this.expressions.Add(new ExpressionProfile
                {
                    category = ExpressionCategory.Select,
                    hasKey = true,
                    hasStartEdge = true,
                    keyType = typeof(TKey),
                    inputType = typeof(TPayload),
                    outputType = typeof(TResult),
                    expression = selector
                });
                return this;
            }
            var prev = this.expressions[this.expressions.Count - 1];
            switch (prev.category)
            {
                case ExpressionCategory.Select:
                {
                    var body = ParameterSubstituter.Replace(selector.Parameters[2], prev.expression.Body, selector.Body);
                    var payloadParameter = prev.expression.Parameters.Last().Yield();

                    IEnumerable<ParameterExpression> startEdgeParameter;
                    if (prev.hasStartEdge)
                    {
                        var startEdge = prev.expression.Parameters[0];
                        body = ParameterSubstituter.Replace(selector.Parameters[0], startEdge, body);
                        startEdgeParameter = startEdge.Yield();
                    }
                    else startEdgeParameter = selector.Parameters[0].Yield();

                    IEnumerable<ParameterExpression> keyParameter;
                    if (prev.hasKey)
                    {
                        var key = prev.expression.Parameters[prev.expression.Parameters.Count - 2];
                        body = ParameterSubstituter.Replace(selector.Parameters[1], key, body);
                        keyParameter = key.Yield();
                    }
                    else keyParameter = selector.Parameters[1].Yield();

                    prev.outputType = typeof(TResult);
                    prev.hasKey = true;
                    prev.keyType = typeof(TKey);
                    prev.expression = Expression.Lambda(body, startEdgeParameter.Concat(keyParameter).Concat(payloadParameter));
                    break;
                }

                default:
                {
                    this.expressions.Add(new ExpressionProfile
                    {
                        category = ExpressionCategory.Select,
                        hasKey = true,
                        hasStartEdge = true,
                        keyType = typeof(TKey),
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                    break;
                }
            }
            return this;
        }

        public FuseModule FuseSelectMany<TPayload, TResult>(Expression<Func<TPayload, IEnumerable<TResult>>> selector)
        {
            if (this.expressions.Count == 0)
            {
                this.expressions.Add(new ExpressionProfile
                {
                    category = ExpressionCategory.SelectMany,
                    inputType = typeof(TPayload),
                    outputType = typeof(TResult),
                    expression = selector
                });
                return this;
            }
            var prev = this.expressions[this.expressions.Count - 1];
            switch (prev.category)
            {
                case ExpressionCategory.Select:
                {
                    var body = selector.ReplaceParametersInBody(prev.expression.Body);

                    prev.category = ExpressionCategory.SelectMany;
                    prev.outputType = typeof(TResult);
                    prev.expression = Expression.Lambda(body, prev.expression.Parameters);
                    break;
                }

                default:
                {
                    this.expressions.Add(new ExpressionProfile
                    {
                        category = ExpressionCategory.SelectMany,
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                    break;
                }
            }
            return this;
        }

        public FuseModule FuseSelectMany<TPayload, TResult>(Expression<Func<long, TPayload, IEnumerable<TResult>>> selector)
        {
            if (this.expressions.Count == 0)
            {
                this.expressions.Add(new ExpressionProfile
                {
                    category = ExpressionCategory.SelectMany,
                    hasStartEdge = true,
                    inputType = typeof(TPayload),
                    outputType = typeof(TResult),
                    expression = selector
                });
                return this;
            }
            var prev = this.expressions[this.expressions.Count - 1];
            switch (prev.category)
            {
                case ExpressionCategory.Select:
                {
                    IEnumerable<ParameterExpression> parameters = prev.expression.Parameters;
                    var body = ParameterSubstituter.Replace(selector.Parameters[1], prev.expression.Body, selector.Body);

                    if (prev.category == ExpressionCategory.Select && prev.hasStartEdge) body = ParameterSubstituter.Replace(selector.Parameters[0], prev.expression.Parameters[0], body);
                    else parameters = selector.Parameters[0].Yield().Concat(parameters);

                    prev.category = ExpressionCategory.SelectMany;
                    prev.outputType = typeof(TResult);
                    prev.expression = Expression.Lambda(body, parameters);
                    break;
                }

                default:
                {
                    this.expressions.Add(new ExpressionProfile
                    {
                        category = ExpressionCategory.SelectMany,
                        hasStartEdge = true,
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                    break;
                }
            }
            return this;
        }

        public FuseModule FuseSelectManyWithKey<TKey, TPayload, TResult>(Expression<Func<TKey, TPayload, IEnumerable<TResult>>> selector)
        {
            if (this.expressions.Count == 0)
            {
                this.expressions.Add(new ExpressionProfile
                {
                    category = ExpressionCategory.SelectMany,
                    hasKey = true,
                    keyType = typeof(TKey),
                    inputType = typeof(TPayload),
                    outputType = typeof(TResult),
                    expression = selector
                });
                return this;
            }
            var prev = this.expressions[this.expressions.Count - 1];
            switch (prev.category)
            {
                case ExpressionCategory.Select:
                {
                    var body = ParameterSubstituter.Replace(selector.Parameters[1], prev.expression.Body, selector.Body);
                    var payloadParameter = prev.expression.Parameters.Last().Yield();

                    var startEdgeParameter = prev.hasStartEdge
                        ? prev.expression.Parameters[0].Yield()
                        : Enumerable.Empty<ParameterExpression>();

                    IEnumerable<ParameterExpression> keyParameter;
                    if (prev.hasKey)
                    {
                        var key = prev.expression.Parameters[prev.expression.Parameters.Count - 2];
                        body = ParameterSubstituter.Replace(selector.Parameters[0], key, body);
                        keyParameter = key.Yield();
                    }
                    else keyParameter = selector.Parameters[0].Yield();

                    prev.category = ExpressionCategory.SelectMany;
                    prev.outputType = typeof(TResult);
                    prev.hasKey = true;
                    prev.keyType = typeof(TKey);
                    prev.expression = Expression.Lambda(body, startEdgeParameter.Concat(keyParameter).Concat(payloadParameter));
                    break;
                }

                default:
                {
                    this.expressions.Add(new ExpressionProfile
                    {
                        category = ExpressionCategory.SelectMany,
                        hasKey = true,
                        keyType = typeof(TKey),
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                    break;
                }
            }
            return this;
        }

        public FuseModule FuseSelectManyWithKey<TKey, TPayload, TResult>(Expression<Func<long, TKey, TPayload, IEnumerable<TResult>>> selector)
        {
            if (this.expressions.Count == 0)
            {
                this.expressions.Add(new ExpressionProfile
                {
                    category = ExpressionCategory.SelectMany,
                    hasKey = true,
                    hasStartEdge = true,
                    keyType = typeof(TKey),
                    inputType = typeof(TPayload),
                    outputType = typeof(TResult),
                    expression = selector
                });
                return this;
            }
            var prev = this.expressions[this.expressions.Count - 1];
            switch (prev.category)
            {
                case ExpressionCategory.Select:
                {
                    var body = ParameterSubstituter.Replace(selector.Parameters[2], prev.expression.Body, selector.Body);
                    var payloadParameter = prev.expression.Parameters.Last().Yield();

                    IEnumerable<ParameterExpression> startEdgeParameter;
                    if (prev.hasStartEdge)
                    {
                        var startEdge = prev.expression.Parameters[0];
                        body = ParameterSubstituter.Replace(selector.Parameters[0], startEdge, body);
                        startEdgeParameter = startEdge.Yield();
                    }
                    else startEdgeParameter = selector.Parameters[0].Yield();

                    IEnumerable<ParameterExpression> keyParameter;
                    if (prev.hasKey)
                    {
                        var key = prev.expression.Parameters[prev.expression.Parameters.Count - 2];
                        body = ParameterSubstituter.Replace(selector.Parameters[1], key, body);
                        keyParameter = key.Yield();
                    }
                    else keyParameter = selector.Parameters[1].Yield();

                    prev.category = ExpressionCategory.SelectMany;
                    prev.outputType = typeof(TResult);
                    prev.hasKey = true;
                    prev.keyType = typeof(TKey);
                    prev.expression = Expression.Lambda(body, startEdgeParameter.Concat(keyParameter).Concat(payloadParameter));
                    break;
                }

                default:
                {
                    this.expressions.Add(new ExpressionProfile
                    {
                        category = ExpressionCategory.SelectMany,
                        hasKey = true,
                        hasStartEdge = true,
                        keyType = typeof(TKey),
                        inputType = typeof(TPayload),
                        outputType = typeof(TResult),
                        expression = selector
                    });
                    break;
                }
            }
            return this;
        }

        public FuseModule FuseWhere<TPayload>(Expression<Func<TPayload, bool>> expression)
        {
            if (this.expressions.Count == 0 || this.expressions[this.expressions.Count - 1].category != ExpressionCategory.Where)
            {
                this.expressions.Add(new ExpressionProfile
                {
                    category = ExpressionCategory.Where,
                    inputType = typeof(TPayload),
                    outputType = typeof(TPayload),
                    expression = expression
                });
                return this;
            }

            var prev = this.expressions[this.expressions.Count - 1];
            var parameter = prev.expression.Parameters[0];
            var replaced = expression.ReplaceParametersInBody(parameter);
            prev.expression = Expression.Lambda<Func<TPayload, bool>>(Expression.AndAlso(prev.expression.Body, replaced), parameter);
            return this;
        }

        public FuseModule FuseSetDurationConstant(long value)
        {
            this.durationAdjustment = Expression.Constant(value);
            return this;
        }

        internal Type InputType => this.expressions.Count == 0 ? null : this.expressions.First().inputType;

        internal Type OutputType => this.expressions.Count == 0 ? null : this.expressions.Last().outputType;

        public bool IsEmpty => this.expressions.Count == 0 && this.durationAdjustment == null;

        public override string ToString()
        {
            string output = string.Empty;
            foreach (var item in this.expressions)
            {
                output = output
                    + "Category:" + item.category.ToString() + ":"
                    + "KeyType:" + (item.keyType?.ToString() ?? "null") + ":"
                    + "OutputType:" + (item.outputType?.ToString() ?? "null") + ":"
                    + "UsesKey:" + item.hasKey.ToString() + ":"
                    + "UsesStartEdge:" + item.hasStartEdge.ToString() + ":"
                    + "Expression:" + item.expression.ExpressionToCSharp() + Environment.NewLine;
            }
            if (this.durationAdjustment != null) output = output + "Duration:" + this.durationAdjustment + Environment.NewLine;
            return output;
        }

        public Expression<Action<long, long, TPayload, TKey>> Coalesce<TPayload, TResult, TKey>(Expression<Action<long, long, TResult, TKey>> action, bool canBePunctuation = false)
        {
            if (this.InputType != null && typeof(TPayload) != this.InputType) throw new InvalidOperationException();
            if (this.OutputType != null && typeof(TResult) != this.OutputType) throw new InvalidOperationException();

            int variableCount = 0;
            var syncParam = action.Parameters[0];
            var otherParam = action.Parameters[1];

            var keyParam = action.Parameters[3];
            var parameter = action.Parameters[2];
            var currentStatement = action.Body;

            for (int index = this.expressions.Count - 1; index >= 0; index--)
            {
                var profile = this.expressions[index];
                switch (profile.category)
                {
                    case ExpressionCategory.Select:
                        {
                            var oldParameter = parameter;
                            parameter = Expression.Variable(profile.inputType, "var" + ++variableCount);

                            var selectBody = profile.expression.Body;
                            if (profile.hasStartEdge) selectBody = ParameterSubstituter.Replace(profile.expression.Parameters[0], syncParam, selectBody);
                            if (profile.hasKey) selectBody = ParameterSubstituter.Replace(profile.expression.Parameters[profile.expression.Parameters.Count - 2], keyParam, selectBody);
                            selectBody = ParameterSubstituter.Replace(profile.expression.Parameters.Last(), parameter, selectBody);

                            currentStatement = Expression.Block(
                                new[] { oldParameter },
                                Expression.Assign(oldParameter, selectBody),
                                currentStatement);
                            break;
                        }

                    case ExpressionCategory.SelectMany:
                        {
                            var oldParameter = parameter;
                            var enumerableParameter = Expression.Variable(profile.expression.ReturnType, "var" + ++variableCount);
                            var indexParameter = Expression.Variable(typeof(int), "var" + ++variableCount);
                            parameter = Expression.Variable(profile.inputType, "var" + ++variableCount);

                            var selectManyBody = profile.expression.Body;
                            if (profile.hasStartEdge) selectManyBody = ParameterSubstituter.Replace(profile.expression.Parameters[0], syncParam, selectManyBody);
                            if (profile.hasKey) selectManyBody = ParameterSubstituter.Replace(profile.expression.Parameters[profile.expression.Parameters.Count - 2], keyParam, selectManyBody);
                            selectManyBody = ParameterSubstituter.Replace(profile.expression.Parameters.Last(), parameter, selectManyBody);

                            var label = Expression.Label();
                            if (enumerableParameter.Type.IsArray)
                            {
                                currentStatement = Expression.Block(
                                    new[] { indexParameter, enumerableParameter },
                                    Expression.Assign(indexParameter, Expression.Constant(0)),
                                    Expression.Assign(enumerableParameter, selectManyBody),
                                    Expression.Loop(
                                        Expression.Block(
                                            new[] { oldParameter },
                                            Expression.IfThen(
                                                Expression.GreaterThanOrEqual(indexParameter, Expression.PropertyOrField(enumerableParameter, "Length")),
                                                Expression.Break(label)),
                                            Expression.Assign(oldParameter, Expression.ArrayIndex(enumerableParameter, indexParameter)),
                                            currentStatement,
                                            Expression.Increment(indexParameter)),
                                    label));
                            }
                            else
                            {
                                var enumerableType = typeof(IEnumerable<>).GetTypeInfo().MakeGenericType(profile.outputType);
                                var enumeratorType = typeof(IEnumerator<>).GetTypeInfo().MakeGenericType(profile.outputType);
                                var enumeratorMethod = enumerableType.GetTypeInfo().GetMethod("GetEnumerator");
                                var enumeratorParameter = Expression.Variable(enumeratorType, enumerableParameter.Name + "Enumerator");
                                var moveNextMethod = typeof(IEnumerator).GetTypeInfo().GetMethod("MoveNext");
                                var disposeMethod = typeof(IDisposable).GetTypeInfo().GetMethod("Dispose");

                                currentStatement = Expression.Block(
                                    new[] { enumerableParameter, enumeratorParameter },
                                    Expression.Assign(enumerableParameter, selectManyBody),
                                    Expression.Assign(enumeratorParameter, Expression.Call(enumerableParameter, enumeratorMethod)),
                                    Expression.Loop(
                                        Expression.Block(
                                            new[] { oldParameter },
                                            Expression.IfThen(
                                                Expression.Not(Expression.Call(enumeratorParameter, moveNextMethod)),
                                                Expression.Break(label)),
                                            Expression.Assign(oldParameter, Expression.Property(enumeratorParameter, "Current")),
                                            currentStatement),
                                        label),
                                    Expression.Call(enumeratorParameter, disposeMethod));
                            }
                            break;
                        }

                    case ExpressionCategory.Where:
                        {
                            currentStatement = Expression.IfThen(
                                profile.expression.ReplaceParametersInBody(parameter),
                                currentStatement);
                            break;
                        }

                    default:
                        throw new InvalidOperationException("Switch statement expected to be exhaustive.");
                }
            }

            if (this.durationAdjustment != null)
            {
                currentStatement = Expression.Block(Expression.IfThen(
                Expression.GreaterThan(otherParam, syncParam),
                Expression.Block(
                    Expression.Assign(
                        otherParam,
                        this.durationAdjustment.ExpressionEquals(Expression.Constant(StreamEvent.InfinitySyncTime))
                            ? this.durationAdjustment
                            : Expression.Add(syncParam, this.durationAdjustment)), currentStatement)));
            }
            if (canBePunctuation)
            {
                currentStatement = Expression.Block(Expression.IfThenElse(
                  Expression.Equal(otherParam, Expression.Constant(long.MinValue)),
                  ParameterSubstituter.Replace(action.Parameters[2], Expression.Constant(default(TResult), typeof(TResult)), action.Body),
                  currentStatement));
            }
            return Expression.Lambda<Action<long, long, TPayload, TKey>>(currentStatement, syncParam, otherParam, parameter, keyParam);
        }

        public string Coalesce<TPayload, TResult, TKey>(string startText, string endText, string payloadText, string keyText, out string leadingText, out string trailingText)
        {
            Expression<Action<long, long, TResult, TKey>> placeholder = (generatedStartTimeVariable, generatedEndTimeVariable, transformedValue, generatedKeyVariable) => PlaceholderMethod.Foo();
            var c = Coalesce<TPayload, TResult, TKey>(placeholder, false);
            var strings = c.Body.ExpressionToCSharp().Split(new string[] { PlaceholderMethod.Text }, StringSplitOptions.None);
            var start = "var " + c.Parameters[0].Name + " = " + startText + ";";
            var end = "var " + c.Parameters[1].Name + " = " + endText + ";";
            var payload = "var " + c.Parameters[2].Name + " = " + payloadText + ";";
            var key = "var " + c.Parameters[3].Name + " = " + keyText + ";";
            leadingText = string.Join(Environment.NewLine, start, end, payload, key, strings[0]);
            trailingText = strings[1];
            if (trailingText[0] == ';') trailingText = trailingText.Substring(1).Trim();
            return placeholder.Parameters[2].Name;
        }
    }
}
