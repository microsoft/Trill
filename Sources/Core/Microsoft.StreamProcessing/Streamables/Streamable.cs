// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// An operator represents a stream query.
    /// </summary>
    /// <typeparam name="TKey">Group key type.</typeparam>
    /// <typeparam name="TPayload">Output payload type.</typeparam>
    [ContractClass(typeof(StreamableContract<,>))]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class Streamable<TKey, TPayload> : IStreamable<TKey, TPayload>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public StreamProperties<TKey, TPayload> Properties => this.properties;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected StreamProperties<TKey, TPayload> properties;

        internal Streamable(StreamProperties<TKey, TPayload> properties)
        {
            Contract.Requires(properties != null);

            this.properties = properties;
        }

        #region IStreamable

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="observer"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public string ErrorMessages => this.errorMessages;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected string errorMessages;
        #endregion
    }

    [ContractClassFor(typeof(Streamable<,>))]
    internal abstract class StreamableContract<TKey, TPayload> : Streamable<TKey, TPayload>
    {
        public StreamableContract(StreamProperties<TKey, TPayload> properties)
            : base(properties)
        { }

        public override IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer)
        {
            Contract.Requires(observer != null);
            return null; // Dummy return
        }
    }
}