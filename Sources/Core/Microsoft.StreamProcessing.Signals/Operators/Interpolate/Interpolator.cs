// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Signal
{
    [DataContract]
    internal abstract class Interpolator<T>
    {
        [DataMember]
        protected readonly long windowSize;

        // Left window point
        [DataMember]
        protected long leftTime;
        [DataMember]
        protected T leftValue;

        // Right window point
        [DataMember]
        protected long rightTime;
        [DataMember]
        protected T rightValue;

        // Number of active samples in window
        [DataMember]
        protected int numActiveSamples;

        protected Interpolator(long windowSize)
        {
            this.windowSize = windowSize;
            this.leftTime = this.rightTime = long.MinValue;
            this.leftValue = this.rightValue = default;
            this.numActiveSamples = 0;
        }

        public long LeftTime
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.leftTime;
        }

        public long RightTime
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.rightTime;
        }

        public long WindowSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.windowSize;
        }

        public int NumActiveSamples
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.numActiveSamples;
        }

        public abstract void AdvanceTime(long time);

        public abstract void AddPoint(long time, ref T value);

        public abstract bool CanInterpolate(long time);

        public abstract void Interpolate(long time, out T value);
    }
}