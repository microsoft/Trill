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

namespace Microsoft.StreamProcessing
{
    internal struct SelectTransformationResult
    {
        public bool Error;
        public IEnumerable<Tuple<MyFieldInfo, MyFieldInfo>> SwingingFields;
        public IDictionary<MyFieldInfo, Expression> ComputedFields;
        public IEnumerable<MyFieldInfo> UnmentionedFields;
        public Expression ProjectionReturningResultInstance;
        public IEnumerable<string> MultiStringOperations;
        public bool NeedsSourceInstance;
    }

    internal struct SelectParameterInformation
    {
        public ColumnarRepresentation parameterRepresentation;
        public string BatchName;
        public Type BatchType;
        public string IndexVariableName;
    }

    internal sealed class SelectTransformer : ExpressionVisitor
    {
        private bool error;
        private readonly List<Tuple<MyFieldInfo, MyFieldInfo>> swingingFields;
        private readonly Dictionary<MyFieldInfo, Expression> computedFields;
        private readonly ColumnarRepresentation resultTypeInformation;
        private readonly bool noSwingingFields;
        private readonly Dictionary<ParameterExpression, SelectParameterInformation> parameterInformation;
        private readonly Expression ProjectionReturningResultInstance;
        private readonly List<string> multiStringOperations;
        private readonly List<MyFieldInfo> multiStringResultFields;
        private readonly bool doMultiStringTransform;
        private bool needsSourceInstance;

        internal static SelectTransformationResult Transform(
            LambdaExpression function,
            IEnumerable<Tuple<ParameterExpression, SelectParameterInformation>> substitutionInformation,
            ColumnarRepresentation resultTypeInformation,
            bool noSwingingFields = false,
            bool hasStartEdge = false)
        {
            Contract.Requires(function != null);
            Contract.Requires(substitutionInformation != null);
            Contract.Requires(resultTypeInformation != null);

            var me = new SelectTransformer(function, substitutionInformation, resultTypeInformation, noSwingingFields,
                Config.UseMultiString && ((Config.MultiStringTransforms & Config.CodegenOptions.MultiStringFlags.VectorOperations) != 0), hasStartEdge);

            // Need to find any unmentioned fields from the result type
            var unmentionedFields = new List<MyFieldInfo>();
            if (me.ProjectionReturningResultInstance == null)
            {
                foreach (var kv in resultTypeInformation.Fields)
                {
                    var fieldInfo = kv.Value;
                    if (me.swingingFields.Any(t => t.Item1.Equals(fieldInfo))) continue;
                    if (me.computedFields.Keys.Any(fi => fi.Equals(fieldInfo))) continue;
                    if (me.multiStringResultFields.Any(f => f.Equals(fieldInfo))) continue;
                    unmentionedFields.Add(fieldInfo);
                }
            }

            return new SelectTransformationResult()
            {
                Error = me.error,
                ComputedFields = me.computedFields,
                SwingingFields = me.swingingFields,
                UnmentionedFields = unmentionedFields,
                ProjectionReturningResultInstance = me.ProjectionReturningResultInstance,
                MultiStringOperations = me.multiStringOperations,
                NeedsSourceInstance = me.needsSourceInstance,
            };
        }

        /// <summary>
        /// Fifth component of tuple is needed when the projection body must be evaluated into a local to hold the result value.
        /// That is the case when the body looks like "new T(...)" or is the result of a method call.
        /// </summary>
        private SelectTransformer(
            LambdaExpression function,
            IEnumerable<Tuple<ParameterExpression, SelectParameterInformation>> substitutionInformation,
            ColumnarRepresentation resultTypeInformation,
            bool noSwingingFields,
            bool doMultiStringTransform,
            bool hasStartEdge)
        {
            this.parameterInformation = new Dictionary<ParameterExpression, SelectParameterInformation>();
            foreach (var tup in substitutionInformation)
            {
                this.parameterInformation.Add(tup.Item1, tup.Item2);
            }
            this.resultTypeInformation = resultTypeInformation;
            this.noSwingingFields = noSwingingFields;
            this.doMultiStringTransform = doMultiStringTransform;

            this.swingingFields = new List<Tuple<MyFieldInfo, MyFieldInfo>>();
            this.computedFields = new Dictionary<MyFieldInfo, Expression>();
            this.multiStringOperations = new List<string>();
            this.multiStringResultFields = new List<MyFieldInfo>();

            var body = function.Body;

            // The projection might just be (e_1, e_2, ..., e_n) => e_i, i.e., projecting just the single parameter e_i.
            if (body is ParameterExpression parameter)
            {
                TransformSingleParameterSelect(parameter, hasStartEdge);
                return;
            }

            // The projection might just be (e_1, e_2, ..., e_n) => e_i.f, i.e., projecting just the single field f from one of the parameters e_i.
            if (body is MemberExpression memberExpression)
            {
                TransformSingleFieldSelect(memberExpression);
                return;
            }

            // Case: projection is (e_1, e_2, ..., e_n) => new { f1 = ..., f2 = ..., ...}), i.e., projecting into an anonymous type
            if (body is NewExpression newExpression && newExpression.Type.IsAnonymousType())
            {
                Contract.Assume(newExpression.Type == resultTypeInformation.RepresentationFor);

                // REVIEW: Should these be turned into part of the if-test?
                Contract.Assume(newExpression.Arguments != null);
                Contract.Assume(newExpression.Members != null);
                Contract.Assume(newExpression.Arguments.Count == newExpression.Members.Count);

                TransformAnonymousTypeSelect(newExpression);
                return;
            }

            // Case: projection is (e_1, e_2, ..., e_n) => new T{ f1 = ..., f2 = ..., ... }), i.e., T is *not* an anonymous type
            // TODO: See if this can be unified with the code above for anonymous types.
            if (body is MemberInitExpression && !this.resultTypeInformation.noFields)
            {
                Visit(body);
                return;
            }

            // Case: projection is (e_1, e_2, ..., e_n) => f(e_1, e_2, ..., e_n)) *AND* the result type is a non-decomposable type, e.g., int.
            // In that case, the expression does not have to be evaluated into a local, but can just be assigned to the result
            // column's pseudo-field, "payload".
            // Note that f is either a real method call or else just an expression
            // that computes a result value (e.g., a type cast which shows up as a unary expression).
            if (this.resultTypeInformation.noFields)
            {
                if (this.doMultiStringTransform && IsMultiStringCall(body, out string s))
                {
                    this.multiStringOperations.Add($"resultBatch.{this.resultTypeInformation.PseudoField.Name} = {s};");
                }
                else
                {
                    var transformedBody = Visit(body);
                    this.computedFields.Add(this.resultTypeInformation.PseudoField, transformedBody);
                }
                return;
            }

            // Case: ValueTuple.Create or Tuple.Create can be transformed to simple columnar assignments
            if (body is MethodCallExpression methodCallBody &&
                (methodCallBody.Method.ReflectedType == typeof(ValueTuple) || methodCallBody.Method.ReflectedType == typeof(Tuple)) &&
                methodCallBody.Method.Name == "Create")
            {
                TransformTupleCreateSelect(methodCallBody);
                return;
            }

            // Otherwise, degenerate case: need to just evaluate the expression (with source field access transformed to columnar).
            // That expression will be passed to the setter for the indexer on the generated batch.
            // REVIEW: this is where something should be signalled so the user knows it isn't as fast as it could be.
            var transformedBody2 = Visit(body);
            this.ProjectionReturningResultInstance = transformedBody2;
            return;
        }

        /// <summary>
        /// Transforms the lambda "(e_1, e_2, ..., e_n) => e_i.f"
        /// (a projection of a single field f from one of the parameters, e_i)
        /// The type of e_i is E_i.
        /// The type of f is the result type R, which is either going to be decomposed into *its* columns or will stay as a single value.
        /// </summary>
        private void TransformSingleFieldSelect(MemberExpression m)
        {
            var fieldOrAutoProp = m.Member;
            if (!(m.Expression is ParameterExpression parameter))
            {
                this.error = true;
                return;
            }
            if (!this.parameterInformation.TryGetValue(parameter, out var selectParameter))
            {
                this.error = true;
                return;
            }
            var columnarField = selectParameter.parameterRepresentation.Fields[fieldOrAutoProp.Name];
            if (this.resultTypeInformation.noFields)
            {
                // Then e.f is of type R (the result type) where R is a primitive type or some type that doesn't get decomposed.
                // In that case, there doesn't need to be a loop at all. The pointer to the column for f can just be swung to the
                // corresponding field in the output batch.
                if (this.noSwingingFields)
                {
                    var a = GetBatchColumnIndexer(parameter, columnarField);
                    this.computedFields.Add(this.resultTypeInformation.PseudoField, a);
                }
                else
                {
                    this.swingingFields.Add(
                        Tuple.Create(this.resultTypeInformation.PseudoField, columnarField));
                }
            }
            else
            {
                // Then e.f is of type R where R is a type that gets decomposed into a column for each field/autoprop.
                // So this has to behave as RowToCol: the value e.f needs to have its subfields assigned to
                // the corresponding columns.
                var indexVariable = GetIndexVariable(parameter);
                foreach (var resultField in this.resultTypeInformation.Fields.Values)
                {
                    var correspondingVariable = Expression.Variable(columnarField.Type.MakeArrayType(), columnarField.Name + "_col");
                    var arrayAccess = Expression.ArrayAccess(correspondingVariable, indexVariable);
                    var fieldDereference = Expression.PropertyOrField(arrayAccess, resultField.OriginalName);
                    this.computedFields.Add(resultField, fieldDereference);
                }
            }
        }

        private ParameterExpression GetIndexVariable(ParameterExpression parameter)
        {
            if (!this.parameterInformation.TryGetValue(parameter, out var selectParameter))
            {
                // then this parameter must be a parameter of an inner lambda or a parameter not being substituted for
                return parameter;
            }
            var indexVariable = Expression.Variable(typeof(int), selectParameter.IndexVariableName);
            return indexVariable;
        }

        private IndexExpression GetBatchColumnIndexer(ParameterExpression parameter, MyFieldInfo f)
        {
            Contract.Requires(this.parameterInformation.ContainsKey(parameter));

            var parameterInfo = this.parameterInformation[parameter];
            var batch = Expression.Variable(parameterInfo.BatchType, parameterInfo.BatchName);
            var column = Expression.Field(batch, f.Name);
            var indexVariable = Expression.Variable(typeof(int), parameterInfo.IndexVariableName);
            if (column.Type.Equals(typeof(Internal.Collections.MultiString)))
            {
                var indexer = typeof(Internal.Collections.MultiString).GetTypeInfo().GetProperty("Item");
                return Expression.MakeIndex(column, indexer, new List<Expression>() { indexVariable });
            }
            else
            {
                var colArray = Expression.Field(column, "col");
                return Expression.ArrayAccess(colArray, indexVariable);
            }
        }

        /// <summary>
        /// Transforms the lambda "(e_1, e_2, ..., e_n) => e_i"
        /// (a projection of a single parameter, e_i)
        /// The type of e_i is E_i.
        /// The type of e_i is also the result type R, which means that we can
        /// just swing the columns representing E_i to the columns representing R.
        /// Special case for when that parameter is the startedge parameter!
        /// </summary>
        private void TransformSingleParameterSelect(ParameterExpression parameter, bool hasStartEdge)
        {
            Contract.Assume(parameter.Type == this.resultTypeInformation.RepresentationFor);

            if (!this.parameterInformation.TryGetValue(parameter, out var selectParameter))
            {
                if (!hasStartEdge && !this.resultTypeInformation.noFields)
                {
                    this.error = true;
                    return;
                }
                else
                {
                    this.computedFields.Add(this.resultTypeInformation.PseudoField, parameter);
                    return;
                }
            }
            foreach (var resultField in this.resultTypeInformation.AllFields)
            {
                var matchingField = selectParameter.parameterRepresentation.AllFields.First(f => f.OriginalName == resultField.OriginalName);
                if (this.noSwingingFields)
                {
                    var a = GetBatchColumnIndexer(parameter, matchingField);
                    this.computedFields.Add(resultField, a);
                }
                else
                {
                    this.swingingFields.Add(
                        Tuple.Create(resultField, matchingField));
                }
            }
        }

        /// <summary>
        /// Transforms the lambda "(e_1, e_2, ..., e_n) => new { f_1 = ..., f_2 = ..., ..., f_m = ... }"
        /// (a projection of an anonymous type)
        /// </summary>
        private void TransformAnonymousTypeSelect(NewExpression newExpression)
        {
            Contract.Requires(newExpression != null);
            Contract.Requires(newExpression.Arguments != null);
            Contract.Requires(newExpression.Members != null);
            Contract.Requires(newExpression.Arguments.Count == newExpression.Members.Count);

            for (int i = 0; i < newExpression.Arguments.Count; i++)
            {
                var member = newExpression.Members[i];
                var destinationField = member as PropertyInfo; // assignments to "fields" of an anonymous type are really to its properties
                Contract.Assume(destinationField != null);
                var argument = newExpression.Arguments[i];

                var resultField = this.resultTypeInformation.Fields[destinationField.Name]; // result type must have Fields

                // Special case for MultiString: right-hand side of the assignment could be a method call
                // (or property, like Length) on a MultiString. This is optimized (but only if
                // this.noSwingingFields is false) into a single call to the associated MultiString method
                // that acts like a swinging field: it is done outside of the loop that operates on each row.
                {
                    if (this.doMultiStringTransform && IsMultiStringCall(argument, out string s))
                    {
                        this.multiStringResultFields.Add(resultField);
                        this.multiStringOperations.Add($"resultBatch.{resultField.Name} = {s};");
                        continue;
                    }
                }

                if (HandleSimpleAssignments(argument, resultField))
                    continue;

                var e = Visit(argument);
                this.computedFields.Add(resultField, e);
            }
        }

        /// <summary>
        /// Transforms the lambda "(e_1, e_2, ..., e_n) => ValueTuple.Create(e_1, e_2, ..., e_n)"
        /// </summary>
        private void TransformTupleCreateSelect(MethodCallExpression methodCall)
        {
            Contract.Requires(methodCall != null);
            Contract.Requires(methodCall.Arguments != null);

            for (int i = 0; i < methodCall.Arguments.Count; i++)
            {
                var argument = methodCall.Arguments[i];
                var resultField = this.resultTypeInformation.Fields[$"Item{i + 1}"];

                // Special case for MultiString: right-hand side of the assignment could be a method call
                // (or property, like Length) on a MultiString. This is optimized (but only if
                // this.noSwingingFields is false) into a single call to the associated MultiString method
                // that acts like a swinging field: it is done outside of the loop that operates on each row.
                if (this.doMultiStringTransform && IsMultiStringCall(argument, out string s))
                {
                    this.multiStringResultFields.Add(resultField);
                    this.multiStringOperations.Add($"resultBatch.{resultField.Name} = {s};");
                }
                else if (!HandleSimpleAssignments(argument, resultField))
                {
                    var e = Visit(argument);
                    this.computedFields.Add(resultField, e);
                }
            }
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            this.error = true;
            return node;
        }

        protected override MemberAssignment VisitMemberAssignment(MemberAssignment node)
        {
            // Supposed to be of the form g = ... for a field g of the result type of the projection.
            var m = node.Member;
            if (!m.IsFieldOrAutoProp())
            {
                this.error = true;
                return node;
            }
            if (!m.DeclaringType.GetTypeInfo().IsAssignableFrom(this.resultTypeInformation.RepresentationFor))
            {
                this.error = true;
                return node;
            }

            var destinationColumn = this.resultTypeInformation.Fields[m.Name];

            if (HandleSimpleAssignments(node.Expression, destinationColumn)) return node;

            // Otherwise it is either a MultiString vector operation or just a point-wise computation of a particular row for g
            if (this.doMultiStringTransform && IsMultiStringCall(node.Expression, out string s))
            {
                this.multiStringResultFields.Add(destinationColumn);
                this.multiStringOperations.Add($"resultBatch.{destinationColumn.Name} = {s};");
            }
            else
            {
                // Transform all occurrences on the right-hand side into a columnar form.
                this.computedFields.Add(destinationColumn, Visit(node.Expression));
            }
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            var member = node.Member;
            if (member.IsFieldOrAutoProp())
            {
                if (node.Expression is ParameterExpression parameter)
                {
                    if (this.parameterInformation.TryGetValue(parameter, out var selectParameter))
                    {
                        var columnarField = selectParameter.parameterRepresentation.Fields[member.Name];
                        var a = GetBatchColumnIndexer(parameter, columnarField);
                        return a;
                    }
                }
            }
            return base.VisitMember(node);
        }

        /// <summary>
        /// If the method call's receiver is a parameter of the original predicate, then
        /// that predicate contained a call like "p.M(...)".
        /// If a method call has an argument that is a parameter of the original predicate,
        /// then that predicate contained a call like "o.M(..., p, ...)".
        ///
        /// Both patterns mean that the predicate cannot be transformed into
        /// a column-oriented view. So just flag the error.
        /// </summary>
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Object is ParameterExpression parameter)
            {
                if (!this.parameterInformation.ContainsKey(parameter)) this.needsSourceInstance = true;
            }
            foreach (var a in node.Arguments)
            {
                parameter = a as ParameterExpression;
                if (parameter != null && this.parameterInformation.ContainsKey(parameter)) this.needsSourceInstance = true;
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (this.parameterInformation.TryGetValue(node, out var selectParameter))
            {
                if (selectParameter.parameterRepresentation.noFields)
                {
                    var columnarField = selectParameter.parameterRepresentation.PseudoField;
                    var a = GetBatchColumnIndexer(node, columnarField);
                    return a;
                }
                else
                    this.needsSourceInstance = true;
            }
            return base.VisitParameter(node);
        }

        /// <summary>
        /// Returns true iff expression is "e.f.M()" or "e.f.P" or
        /// "e.M()" or "e.P" where
        /// e is a parameter of the select function
        /// f is a field/property on the batch type
        /// M is a method from System.String for which there is a MultiString method
        /// P is a property from System.String for which there is a MultiString method
        /// </summary>
        private static bool IsMultiStringCall(Expression expression, out string vectorCall)
        {
            vectorCall = null;
            var methodCall = expression as MethodCallExpression;
            var memberBinding = expression as MemberExpression;
            if (methodCall == null && memberBinding == null) return false;
            Contract.Assume(methodCall == null || memberBinding == null, "supposed to be exclusive and exhaustive");
            ParameterExpression e;
            MemberInfo memberInfo = null;
            if (methodCall != null)
            {
                if (!methodCall.Method.DeclaringType.Equals(typeof(string))) return false;
                if (!MultiStringHasImplementation(methodCall.Method.Name)) return false;

                e = methodCall.Object as ParameterExpression;
                if (e == null)
                {
                    if (methodCall.Object is MemberExpression m2)
                    {
                        e = m2.Expression as ParameterExpression;
                        memberInfo = m2.Member;
                    }
                }
                if (e == null) return false;

                var s = $"sourceBatch.{Transformer.ColumnFieldPrefix}{memberInfo.Name}.{MapStringArgsToMultiStringArgs(methodCall)}";
                vectorCall = s;
                return true;
            }
            else
            {
                // memberBinding != null
                if (!memberBinding.Member.DeclaringType.Equals(typeof(string))) return false;
                if (!MultiStringHasImplementation(memberBinding.Member.Name)) return false;

                e = memberBinding.Expression as ParameterExpression;
                if (e == null)
                {
                    var mb2 = memberBinding.Expression as MemberExpression;
                    e = mb2.Expression as ParameterExpression;
                    memberInfo = mb2.Member;
                }
                if (e == null) return false;

                var s = $"{Transformer.ColumnFieldPrefix}{memberInfo.Name}_col.{memberBinding.Member.Name}(batch.bitvector, false);";
                vectorCall = s;
                return true;
            }
        }

        /// <summary>
        /// Returns true iff MultiString has an implementation which can be used
        /// on an entire batch column and which returns a value which can be assigned
        /// to a column in the result batch. So Contains and Equals don't count because
        /// they return an occupancy vector (i.e., a bit vector) and not a column of
        /// bool.
        /// </summary>
        private static bool MultiStringHasImplementation(string methodName)
        {
            switch (methodName)
            {
                case "GetHashCode":
                case "IndexOf":
                case "LastIndexOf":
                case "Length":
                case "Split":
                case "Substring":
                case "ToLower":
                case "ToUpper":
                    return true;
                default:
                    return false;
            }
        }

        private static string MapStringArgsToMultiStringArgs(MethodCallExpression methodCall)
        {
            var methodName = methodCall.Method.Name;
            string methodToCall = null;
            var args = new List<string>(
                methodCall
                .Arguments
                .Select(a => a.ExpressionToCSharp()));
            string firstArgsToMultiStringCall = null;

            switch (methodName)
            {
                case "Contains":
                case "Equals":
                case "Substring":
                    methodToCall = methodName;
                    firstArgsToMultiStringCall = string.Join(",", args);
                    break;

                case "IndexOf":
                case "LastIndexOf":
                    // need to decide on which overload of IndexOf/LastIndexOf was used
                    methodToCall = methodName;
                    var firstArgAsCSharpString = args[0];
                    var firstArgIsChar = methodCall.Arguments.ElementAt(0).Type.Equals(typeof(char));
                    var n = methodCall.Arguments.Count;
                    if (n == 1)
                    {
                        firstArgsToMultiStringCall = firstArgIsChar
                            ? $"{firstArgAsCSharpString}.ToString(), 0, StringComparison.Ordinal"
                            : $"{firstArgAsCSharpString}, 0, StringComparison.Ordinal";
                    }
                    else
                    {
                        var secondArgAsCSharpString = args[1];
                        if (n == 2)
                        {
                            if (methodCall.Arguments.ElementAt(1).Type.Equals(typeof(int)))
                            {
                                firstArgsToMultiStringCall = firstArgIsChar
                                    ? $"{firstArgAsCSharpString}.ToString(), {secondArgAsCSharpString}, StringComparison.Ordinal"
                                    : $"{firstArgAsCSharpString}, {secondArgAsCSharpString}, StringComparison.Ordinal";
                            }
                            else
                            {
                                // IndexOf/LastIndexOf(string, StringComparison)
                                firstArgsToMultiStringCall = $"{firstArgAsCSharpString}, 0, {secondArgAsCSharpString}";
                            }
                        }
                        else
                        {
                            var thirdArgAsCSharpString = args[2];
                            if (n == 3)
                            {
                                if (firstArgIsChar) // IndexOf/LastIndexOf(char, int, int)
                                    firstArgsToMultiStringCall = $"{firstArgAsCSharpString}.ToString(), {secondArgAsCSharpString}, {thirdArgAsCSharpString}, StringComparison.Ordinal";
                                else
                                {
                                    firstArgsToMultiStringCall = methodCall.Method.GetParameters().ElementAt(2).ParameterType.Equals(typeof(int))
                                        ? $"{firstArgAsCSharpString}, {secondArgAsCSharpString}, {thirdArgAsCSharpString}, StringComparison.Ordinal"
                                        : $"{firstArgAsCSharpString}, {secondArgAsCSharpString}, {thirdArgAsCSharpString}";
                                }
                            }
                            else
                            {
                                Contract.Assume(n == 4, "meant to be exhaustive");
                                var fourthArgAsCSharpString = args[3];

                                // IndexOf/LastIndexOf(string, int, int, StringComparison)
                                firstArgsToMultiStringCall = $"{firstArgAsCSharpString}, {secondArgAsCSharpString}, {thirdArgAsCSharpString}, {fourthArgAsCSharpString}";
                            }
                        }
                    }
                    break;

                default:
                    Contract.Assume(false, "case meant to be exhaustive");
                    break;

            }
            var s = methodName.Equals("Substring")
                ? $"{methodToCall}({firstArgsToMultiStringCall}, sourceBatch.bitvector)"
                : $"{methodToCall}({firstArgsToMultiStringCall}, sourceBatch.bitvector, false)";
            return s;
        }

        /// <summary>
        /// Two special cases for the right-hand side of an assignment or argument to a new expression where it turns into a column swing:
        ///    g = e_i.f
        ///        Then the projection for g turns into a swing of the column from f
        ///        E_i must be a decomposable type
        ///    g = e_i
        ///        The the projection for g turns into a swing of the pseudo-column for E_i
        ///        E_i must be an atomic type
        /// </summary>
        /// <returns>True iff the expression was transformed, either into a computed field or a swinging field.</returns>
        private bool HandleSimpleAssignments(Expression e, MyFieldInfo destinationColumn)
        {
            var simpleAssignedValue = e as MemberExpression;
            ParameterExpression parameter = null;
            if (simpleAssignedValue != null) parameter = simpleAssignedValue.Expression as ParameterExpression;
            var paramIsForThisLambda = parameter != null && this.parameterInformation.ContainsKey(parameter);
            if (simpleAssignedValue != null && paramIsForThisLambda)
            {
                if (!this.parameterInformation.TryGetValue(parameter, out SelectParameterInformation spi))
                {
                    // if this particular parameter is not being substituted for, then the caller must not want it turned
                    // into anything. however, i think it might be an error situation and "this.error" should be set to true.
                    this.computedFields.Add(destinationColumn, simpleAssignedValue);
                    return true;
                }
                var columnarField = spi.parameterRepresentation.Fields[simpleAssignedValue.Member.Name];
                if (this.noSwingingFields)
                {
                    var a = GetBatchColumnIndexer(parameter, columnarField);
                    this.computedFields.Add(destinationColumn, a);
                }
                else
                    this.swingingFields.Add(Tuple.Create(destinationColumn, columnarField));
                return true;
            }
            parameter = e as ParameterExpression;
            if (parameter != null && this.parameterInformation.ContainsKey(parameter))
            {
                if (this.parameterInformation.TryGetValue(parameter, out SelectParameterInformation spi))
                {
                    var cr = spi.parameterRepresentation;
                    if (!cr.noFields)
                    {
                        this.error = true;
                        return false;
                    }
                    if (this.noSwingingFields)
                    {
                        var a = GetBatchColumnIndexer(parameter, cr.PseudoField);
                        this.computedFields.Add(destinationColumn, a);
                    }
                    else
                        this.swingingFields.Add(Tuple.Create(destinationColumn, cr.PseudoField));
                    return true;
                }
            }
            return false;
        }
    }
}
