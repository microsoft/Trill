// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Signal
{
    /// <summary>
    /// Wrapper class for holding a uniformly-sampled signal streamable
    /// </summary>
    /// <typeparam name="TKey">Grouping key type</typeparam>
    /// <typeparam name="TPayload">Data/event payload type</typeparam>
    public class UniformSignal<TKey, TPayload>
    {
        /// <summary>
        /// Provides access to the underlying data stream
        /// </summary>
        protected IStreamable<TKey, TPayload> stream;

        internal UniformSignal(IStreamable<TKey, TPayload> stream, UniformSignalProperties<TKey, TPayload> properties)
        {
            Contract.Requires(stream != null);

            this.stream = stream.SetProperties(properties);
        }

        /// <summary>
        /// Provides access to the underlying data stream
        /// </summary>
        public IStreamable<TKey, TPayload> Stream => stream;

        internal UniformSignalProperties<TKey, TPayload> Properties => (UniformSignalProperties<TKey, TPayload>)stream.Properties;

        /// <summary>
        /// Provides access to the uniform signal's sample period
        /// </summary>
        public long Period => Properties.Period;

        /// <summary>
        /// Provides access to the uniform signal's sample offset
        /// </summary>
        public long Offset => Properties.Offset;

        /// <summary>
        /// Provides access to the uniform signal's interpolation policy
        /// </summary>
        public InterpolationPolicy<TPayload> InterpolationPolicy => Properties.InterpolationPolicy;

        /// <summary>
        /// Performs a relational project over a uniform signal streamable.
        /// <param name="selector">A selector used to transform data via expression</param>
        /// <returns>The transformed uniform signal</returns>
        /// </summary>
        public UniformSignal<TKey, TPayload> Select(Expression<Func<TPayload, TPayload>> selector)
        {
            var stream = Stream.Select(selector);
            var properties = new UniformSignalProperties<TKey, TPayload>(
                stream.Properties,
                Period, Offset, InterpolationPolicy);

            return new UniformSignal<TKey, TPayload>(stream, properties);
        }

        /// <summary>
        /// Performs a relational project over a uniform signal streamable.
        /// <param name="selector">A selector used to transform data via expression</param>
        /// <returns>The transformed uniform signal</returns>
        /// </summary>
        public UniformSignal<TKey, TPayload> Select(Expression<Func<long, TPayload, TPayload>> selector)
        {
            var stream = Stream.Select(selector);
            var properties = new UniformSignalProperties<TKey, TPayload>(
                stream.Properties,
                Period, Offset, InterpolationPolicy);

            return new UniformSignal<TKey, TPayload>(stream, properties);
        }

        /// <summary>
        /// Performs a filter over a uniform signal streamable, excluding samples for which the predicate evaluates to false.
        /// <param name="predicate">A predicate used to filter data via expression</param>
        /// <returns>The transformed uniform signal</returns>
        /// </summary>
        public UniformSignal<TKey, TPayload> Where(Expression<Func<TPayload, bool>> predicate)
        {
            var stream = Stream.Where(predicate);
            var properties = new UniformSignalProperties<TKey, TPayload>(stream.Properties, Period, Offset, InterpolationPolicy);

            return new UniformSignal<TKey, TPayload>(stream, properties);
        }

        /// <summary>
        /// Performs a scaling of all data over a uniform signal streamable.
        /// <param name="factor">The numeric factor to scale the streamable</param>
        /// <returns>The transformed uniform signal</returns>
        /// </summary>
        public UniformSignal<TKey, TPayload> Scale(double factor)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("*d"), "Type " + typeof(TPayload) + " does not support the multiplication operator with second argument " + typeof(double) + ".");
            if (factor == 1.0) return this;

            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Constant(factor);
            var multiply = Expression.Multiply(leftParam, rightParam);
            return Select(Expression.Lambda<Func<TPayload, TPayload>>(multiply, leftParam));
        }

        /// <summary>
        /// Add the values in two uniform signals
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The sum of the two uniform signals</returns>
        public static UniformSignal<TKey, TPayload> operator +(UniformSignal<TKey, TPayload> left, UniformSignal<TKey, TPayload> right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("+"), "Type " + typeof(TPayload) + " does not support the addition operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Parameter(typeof(TPayload));
            var add = Expression.Add(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TPayload, TPayload, TPayload>>(add, leftParam, rightParam), null);
        }

        /// <summary>
        /// Add a constant to the values in a uniform signal
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The sum of the uniform signal and the constant</returns>
        public static UniformSignal<TKey, TPayload> operator +(UniformSignal<TKey, TPayload> left, TPayload right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("+"), "Type " + typeof(TPayload) + " does not support the addition operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Constant(right);
            var add = Expression.Add(leftParam, rightParam);
            return left.Select(Expression.Lambda<Func<TPayload, TPayload>>(add, leftParam));
        }

        /// <summary>
        /// Find the difference between the values in two uniform signals
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The difference of the two uniform signals</returns>
        public static UniformSignal<TKey, TPayload> operator -(UniformSignal<TKey, TPayload> left, UniformSignal<TKey, TPayload> right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("-"), "Type " + typeof(TPayload) + " does not support the subtraction operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Parameter(typeof(TPayload));
            var subtract = Expression.Subtract(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TPayload, TPayload, TPayload>>(subtract, leftParam, rightParam), null);
        }

        /// <summary>
        /// Find the difference between a uniform signal and a constant
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The difference between the uniform signal and the constant</returns>
        public static UniformSignal<TKey, TPayload> operator -(UniformSignal<TKey, TPayload> left, TPayload right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("-"), "Type " + typeof(TPayload) + " does not support the subtraction operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Constant(right);
            var subtract = Expression.Subtract(leftParam, rightParam);
            return left.Select(Expression.Lambda<Func<TPayload, TPayload>>(subtract, leftParam));
        }

        /// <summary>
        /// Multiply the values in two uniform signals
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The product of the two uniform signals</returns>
        public static UniformSignal<TKey, TPayload> operator *(UniformSignal<TKey, TPayload> left, UniformSignal<TKey, TPayload> right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("*"), "Type " + typeof(TPayload) + " does not support the multiplication operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Parameter(typeof(TPayload));
            var multiply = Expression.Multiply(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TPayload, TPayload, TPayload>>(multiply, leftParam, rightParam), null);
        }

        /// <summary>
        /// Multiply a uniform signal and a constant
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The product between the uniform signal and the constant</returns>
        public static UniformSignal<TKey, TPayload> operator *(UniformSignal<TKey, TPayload> left, TPayload right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("*"), "Type " + typeof(TPayload) + " does not support the multiplication operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Constant(right);
            var multiply = Expression.Multiply(leftParam, rightParam);
            return left.Select(Expression.Lambda<Func<TPayload, TPayload>>(multiply, leftParam));
        }

        /// <summary>
        /// Divide the values in two uniform signals
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The quotient of the two uniform signals</returns>
        public static UniformSignal<TKey, TPayload> operator /(UniformSignal<TKey, TPayload> left, UniformSignal<TKey, TPayload> right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("/"), "Type " + typeof(TPayload) + " does not support the division operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Parameter(typeof(TPayload));
            var divide = Expression.Divide(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TPayload, TPayload, TPayload>>(divide, leftParam, rightParam), null);
        }

        /// <summary>
        /// Dividy a uniform signal by a constant
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The quotient between the uniform signal and the constant</returns>
        public static UniformSignal<TKey, TPayload> operator /(UniformSignal<TKey, TPayload> left, TPayload right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("/"), "Type " + typeof(TPayload) + " does not support the division operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Constant(right);
            var divide = Expression.Divide(leftParam, rightParam);
            return left.Select(Expression.Lambda<Func<TPayload, TPayload>>(divide, leftParam));
        }

        /// <summary>
        /// Modulus the values in two uniform signals
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The modulus between the two uniform signals</returns>
        public static UniformSignal<TKey, TPayload> operator %(UniformSignal<TKey, TPayload> left, UniformSignal<TKey, TPayload> right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("%"), "Type " + typeof(TPayload) + " does not support the modulus operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Parameter(typeof(TPayload));
            var modulo = Expression.Modulo(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TPayload, TPayload, TPayload>>(modulo, leftParam, rightParam), null);
        }

        /// <summary>
        /// Modulus a uniform signal by a constant
        /// </summary>
        /// <param name="left">Left argument to the arithmetic operation</param>
        /// <param name="right">Right argument to the arithmetic operation</param>
        /// <returns>The modulus between the uniform signal and the constant</returns>

        public static UniformSignal<TKey, TPayload> operator %(UniformSignal<TKey, TPayload> left, TPayload right)
        {
            Invariant.IsTrue(typeof(TPayload).SupportsOperator("%"), "Type " + typeof(TPayload) + " does not support the modulus operator with second argument " + typeof(TPayload) + ".");
            var leftParam = Expression.Parameter(typeof(TPayload));
            var rightParam = Expression.Constant(right);
            var modulo = Expression.Modulo(leftParam, rightParam);
            return left.Select(Expression.Lambda<Func<TPayload, TPayload>>(modulo, leftParam));
        }
    }
}
