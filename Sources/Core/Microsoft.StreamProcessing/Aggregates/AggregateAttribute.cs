// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;

namespace Microsoft.StreamProcessing.Aggregates
{
    /// <summary>
    /// Provides runtime information on what aggregate implementations may be available for a method
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public sealed class AggregateAttribute : Attribute
    {
        /// <summary>
        /// The type information for the aggregate class to be used for calculating the aggregation
        /// </summary>
        public Type Aggregate { get; }

        /// <summary>
        /// Creates a new hint to the query compiler as to what aggregate to use for the given method
        /// </summary>
        /// <param name="type">A reference to the class that implements the interface IAggregate</param>
        public AggregateAttribute(Type type)
        {
            if (!type.IsClass)
                throw new ArgumentException("The type parameter to the Aggregate attribute must refer to a class.");
            if (!type.GetInterfaces().Any(i => i.GetGenericTypeDefinition() == typeof(IAggregate<,,>)))
                throw new ArgumentException("The type parameter must refer to a class that implements the aggregate interface.");
            if (!type.GetConstructors().Any(c => c.GetParameters().Length == 0))
                throw new ArgumentException("The type parameter must refer to a class that has a parameterless constructor.");
            this.Aggregate = type;
        }
    }
}
