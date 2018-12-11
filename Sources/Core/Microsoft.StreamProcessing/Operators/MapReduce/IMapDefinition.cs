// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Gives a hint as to what methods should be used for an operator
    /// </summary>
    public enum OperationalHint
    {
        /// <summary>
        /// No hint given. So, for instance:
        /// - Use a standard two-input reducer that partitions data by hash buckets.
        /// - Use a standard streaming symmetric hash join operator.
        /// </summary>
        None,

        /// <summary>
        /// Assume that the pair of inputs are vastly different in size, with the left input much smaller.
        /// - Use a two-input reducer where the left side input is multicast to every reducer instance, and the right side is sprayed across instances.
        /// - Use a variant of join where the left side is much smaller than the right side, which will allow us to optimize join on multiple cores by multicasting the smaller side to every instance of the join operator.
        /// </summary>
        Asymmetric
    }

    /// <summary>
    /// Placeholder interface for an operation that represents a map-reduce function
    /// </summary>
    /// <typeparam name="TOuterKey">Type of the outer grouping key</typeparam>
    /// <typeparam name="TMapInputLeft">Type of the left input to the map-reduce</typeparam>
    /// <typeparam name="TMapInputRight">Type of the right input to the map-reduce</typeparam>
    /// <typeparam name="TInnerKey">Type of the inner grouping key</typeparam>
    /// <typeparam name="TReduceInput">Type of the data fed to the reducer</typeparam>
    public interface IMapDefinition<TOuterKey, TMapInputLeft, TMapInputRight, TInnerKey, TReduceInput>
    { }

    /// <summary>
    /// Placeholder interface for an operation that represents a map-reduce function
    /// </summary>
    /// <typeparam name="TMapInputLeft">Type of the left input to the map-reduce</typeparam>
    /// <typeparam name="TMapInputRight">Type of the right input to the map-reduce</typeparam>
    /// <typeparam name="TInnerKey">Type of the inner grouping key</typeparam>
    /// <typeparam name="TReduceInput">Type of the data fed to the reducer</typeparam>
    public interface IMapDefinition<TMapInputLeft, TMapInputRight, TInnerKey, TReduceInput>
    { }
}
