// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal abstract class BinaryStreamable<TKey, TLeft, TRight, TResult> : Streamable<TKey, TResult>
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        public IStreamable<TKey, TLeft> Left;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        public IStreamable<TKey, TRight> Right;

        private readonly bool registerInputs;

        protected BinaryStreamable(StreamProperties<TKey, TResult> properties, IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right, bool registerInputs = false)
            : base(properties)
        {
            Contract.Requires(left != null);
            Contract.Requires(right != null);

            this.Left = left;
            this.Right = right;
            this.registerInputs = registerInputs;
        }

        public override sealed IDisposable Subscribe(IStreamObserver<TKey, TResult> observer)
        {
            var binaryPipe = CreatePipe(observer);
            return Utility.CreateDisposable(this.Left.Subscribe(this.registerInputs ? Config.StreamScheduler.RegisterStreamObserver(binaryPipe.Left) : binaryPipe.Left), this.Right.Subscribe(this.registerInputs ? Config.StreamScheduler.RegisterStreamObserver(binaryPipe.Right) : binaryPipe.Right));
        }

        protected abstract IBinaryObserver<TKey, TLeft, TRight, TResult> CreatePipe(IStreamObserver<TKey, TResult> observer);

        protected void Initialize()
        {
            if (this.Left.Properties.IsColumnar && this.Right.Properties.IsColumnar)
            {
                if (!CanGenerateColumnar())
                {
                    this.properties = this.properties.ToRowBased();
                    this.Left = this.Left.ColumnToRow();
                    this.Right = this.Right.ColumnToRow();
                }
            }
            else if (this.Left.Properties.IsColumnar)
            {
                this.Left = this.Left.ColumnToRow();
            }
            else if (this.Right.Properties.IsColumnar)
            {
                this.Right = this.Right.ColumnToRow();
            }
        }

        protected abstract bool CanGenerateColumnar();
    }
}