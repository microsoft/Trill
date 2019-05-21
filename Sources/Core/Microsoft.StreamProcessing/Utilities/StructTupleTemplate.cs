// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System;
using System.ComponentModel;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Internal
{
    internal static class StructTuple
    {
        public static StructTuple<T1> Create<T1>(T1 item1)
        {
            return new StructTuple<T1>() { Item1 = item1 };
        }
        public static StructTuple<T1, T2> Create<T1, T2>(T1 item1, T2 item2)
        {
            return new StructTuple<T1, T2>() { Item1 = item1, Item2 = item2 };
        }
        public static StructTuple<T1, T2, T3> Create<T1, T2, T3>(T1 item1, T2 item2, T3 item3)
        {
            return new StructTuple<T1, T2, T3>() { Item1 = item1, Item2 = item2, Item3 = item3 };
        }
        public static StructTuple<T1, T2, T3, T4> Create<T1, T2, T3, T4>(T1 item1, T2 item2, T3 item3, T4 item4)
        {
            return new StructTuple<T1, T2, T3, T4>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4 };
        }
        public static StructTuple<T1, T2, T3, T4, T5> Create<T1, T2, T3, T4, T5>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5)
        {
            return new StructTuple<T1, T2, T3, T4, T5>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6> Create<T1, T2, T3, T4, T5, T6>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7> Create<T1, T2, T3, T4, T5, T6, T7>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7, T8> Create<T1, T2, T3, T4, T5, T6, T7, T8>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, T8 item8)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7, T8>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7, Item8 = item8 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, T8 item8, T9 item9)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7, Item8 = item8, Item9 = item9 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, T8 item8, T9 item9, T10 item10)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7, Item8 = item8, Item9 = item9, Item10 = item10 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, T8 item8, T9 item9, T10 item10, T11 item11)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7, Item8 = item8, Item9 = item9, Item10 = item10, Item11 = item11 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, T8 item8, T9 item9, T10 item10, T11 item11, T12 item12)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7, Item8 = item8, Item9 = item9, Item10 = item10, Item11 = item11, Item12 = item12 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, T8 item8, T9 item9, T10 item10, T11 item11, T12 item12, T13 item13)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7, Item8 = item8, Item9 = item9, Item10 = item10, Item11 = item11, Item12 = item12, Item13 = item13 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, T8 item8, T9 item9, T10 item10, T11 item11, T12 item12, T13 item13, T14 item14)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7, Item8 = item8, Item9 = item9, Item10 = item10, Item11 = item11, Item12 = item12, Item13 = item13, Item14 = item14 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, T8 item8, T9 item9, T10 item10, T11 item11, T12 item12, T13 item13, T14 item14, T15 item15)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7, Item8 = item8, Item9 = item9, Item10 = item10, Item11 = item11, Item12 = item12, Item13 = item13, Item14 = item14, Item15 = item15 };
        }
        public static StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, T8 item8, T9 item9, T10 item10, T11 item11, T12 item12, T13 item13, T14 item14, T15 item15, T16 item16)
        {
            return new StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>() { Item1 = item1, Item2 = item2, Item3 = item3, Item4 = item4, Item5 = item5, Item6 = item6, Item7 = item7, Item8 = item8, Item9 = item9, Item10 = item10, Item11 = item11, Item12 = item12, Item13 = item13, Item14 = item14, Item15 = item15, Item16 = item16 };
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 1 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 2 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 3 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 4 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 5 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 6 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 7 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 8 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    /// <typeparam name="T8">Type of component 8 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7, T8> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Item number 8 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T8 Item8;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7, this.Item8 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
           (this.Item8 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 9 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    /// <typeparam name="T8">Type of component 8 of the tuple.</typeparam>
    /// <typeparam name="T9">Type of component 9 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Item number 8 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T8 Item8;

        /// <summary>
        /// Item number 9 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T9 Item9;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7, this.Item8, this.Item9 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
           (this.Item8 as IDisposable)?.Dispose();
           (this.Item9 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 10 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    /// <typeparam name="T8">Type of component 8 of the tuple.</typeparam>
    /// <typeparam name="T9">Type of component 9 of the tuple.</typeparam>
    /// <typeparam name="T10">Type of component 10 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Item number 8 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T8 Item8;

        /// <summary>
        /// Item number 9 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T9 Item9;

        /// <summary>
        /// Item number 10 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T10 Item10;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7, this.Item8, this.Item9, this.Item10 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
           (this.Item8 as IDisposable)?.Dispose();
           (this.Item9 as IDisposable)?.Dispose();
           (this.Item10 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 11 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    /// <typeparam name="T8">Type of component 8 of the tuple.</typeparam>
    /// <typeparam name="T9">Type of component 9 of the tuple.</typeparam>
    /// <typeparam name="T10">Type of component 10 of the tuple.</typeparam>
    /// <typeparam name="T11">Type of component 11 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Item number 8 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T8 Item8;

        /// <summary>
        /// Item number 9 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T9 Item9;

        /// <summary>
        /// Item number 10 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T10 Item10;

        /// <summary>
        /// Item number 11 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T11 Item11;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7, this.Item8, this.Item9, this.Item10, this.Item11 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
           (this.Item8 as IDisposable)?.Dispose();
           (this.Item9 as IDisposable)?.Dispose();
           (this.Item10 as IDisposable)?.Dispose();
           (this.Item11 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 12 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    /// <typeparam name="T8">Type of component 8 of the tuple.</typeparam>
    /// <typeparam name="T9">Type of component 9 of the tuple.</typeparam>
    /// <typeparam name="T10">Type of component 10 of the tuple.</typeparam>
    /// <typeparam name="T11">Type of component 11 of the tuple.</typeparam>
    /// <typeparam name="T12">Type of component 12 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Item number 8 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T8 Item8;

        /// <summary>
        /// Item number 9 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T9 Item9;

        /// <summary>
        /// Item number 10 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T10 Item10;

        /// <summary>
        /// Item number 11 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T11 Item11;

        /// <summary>
        /// Item number 12 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T12 Item12;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7, this.Item8, this.Item9, this.Item10, this.Item11, this.Item12 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
           (this.Item8 as IDisposable)?.Dispose();
           (this.Item9 as IDisposable)?.Dispose();
           (this.Item10 as IDisposable)?.Dispose();
           (this.Item11 as IDisposable)?.Dispose();
           (this.Item12 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 13 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    /// <typeparam name="T8">Type of component 8 of the tuple.</typeparam>
    /// <typeparam name="T9">Type of component 9 of the tuple.</typeparam>
    /// <typeparam name="T10">Type of component 10 of the tuple.</typeparam>
    /// <typeparam name="T11">Type of component 11 of the tuple.</typeparam>
    /// <typeparam name="T12">Type of component 12 of the tuple.</typeparam>
    /// <typeparam name="T13">Type of component 13 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Item number 8 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T8 Item8;

        /// <summary>
        /// Item number 9 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T9 Item9;

        /// <summary>
        /// Item number 10 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T10 Item10;

        /// <summary>
        /// Item number 11 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T11 Item11;

        /// <summary>
        /// Item number 12 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T12 Item12;

        /// <summary>
        /// Item number 13 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T13 Item13;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7, this.Item8, this.Item9, this.Item10, this.Item11, this.Item12, this.Item13 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
           (this.Item8 as IDisposable)?.Dispose();
           (this.Item9 as IDisposable)?.Dispose();
           (this.Item10 as IDisposable)?.Dispose();
           (this.Item11 as IDisposable)?.Dispose();
           (this.Item12 as IDisposable)?.Dispose();
           (this.Item13 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 14 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    /// <typeparam name="T8">Type of component 8 of the tuple.</typeparam>
    /// <typeparam name="T9">Type of component 9 of the tuple.</typeparam>
    /// <typeparam name="T10">Type of component 10 of the tuple.</typeparam>
    /// <typeparam name="T11">Type of component 11 of the tuple.</typeparam>
    /// <typeparam name="T12">Type of component 12 of the tuple.</typeparam>
    /// <typeparam name="T13">Type of component 13 of the tuple.</typeparam>
    /// <typeparam name="T14">Type of component 14 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Item number 8 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T8 Item8;

        /// <summary>
        /// Item number 9 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T9 Item9;

        /// <summary>
        /// Item number 10 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T10 Item10;

        /// <summary>
        /// Item number 11 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T11 Item11;

        /// <summary>
        /// Item number 12 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T12 Item12;

        /// <summary>
        /// Item number 13 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T13 Item13;

        /// <summary>
        /// Item number 14 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T14 Item14;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7, this.Item8, this.Item9, this.Item10, this.Item11, this.Item12, this.Item13, this.Item14 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
           (this.Item8 as IDisposable)?.Dispose();
           (this.Item9 as IDisposable)?.Dispose();
           (this.Item10 as IDisposable)?.Dispose();
           (this.Item11 as IDisposable)?.Dispose();
           (this.Item12 as IDisposable)?.Dispose();
           (this.Item13 as IDisposable)?.Dispose();
           (this.Item14 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 15 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    /// <typeparam name="T8">Type of component 8 of the tuple.</typeparam>
    /// <typeparam name="T9">Type of component 9 of the tuple.</typeparam>
    /// <typeparam name="T10">Type of component 10 of the tuple.</typeparam>
    /// <typeparam name="T11">Type of component 11 of the tuple.</typeparam>
    /// <typeparam name="T12">Type of component 12 of the tuple.</typeparam>
    /// <typeparam name="T13">Type of component 13 of the tuple.</typeparam>
    /// <typeparam name="T14">Type of component 14 of the tuple.</typeparam>
    /// <typeparam name="T15">Type of component 15 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Item number 8 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T8 Item8;

        /// <summary>
        /// Item number 9 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T9 Item9;

        /// <summary>
        /// Item number 10 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T10 Item10;

        /// <summary>
        /// Item number 11 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T11 Item11;

        /// <summary>
        /// Item number 12 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T12 Item12;

        /// <summary>
        /// Item number 13 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T13 Item13;

        /// <summary>
        /// Item number 14 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T14 Item14;

        /// <summary>
        /// Item number 15 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T15 Item15;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7, this.Item8, this.Item9, this.Item10, this.Item11, this.Item12, this.Item13, this.Item14, this.Item15 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
           (this.Item8 as IDisposable)?.Dispose();
           (this.Item9 as IDisposable)?.Dispose();
           (this.Item10 as IDisposable)?.Dispose();
           (this.Item11 as IDisposable)?.Dispose();
           (this.Item12 as IDisposable)?.Dispose();
           (this.Item13 as IDisposable)?.Dispose();
           (this.Item14 as IDisposable)?.Dispose();
           (this.Item15 as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A simple structure representing a tuple of 16 items.
    /// </summary>
    /// <typeparam name="T1">Type of component 1 of the tuple.</typeparam>
    /// <typeparam name="T2">Type of component 2 of the tuple.</typeparam>
    /// <typeparam name="T3">Type of component 3 of the tuple.</typeparam>
    /// <typeparam name="T4">Type of component 4 of the tuple.</typeparam>
    /// <typeparam name="T5">Type of component 5 of the tuple.</typeparam>
    /// <typeparam name="T6">Type of component 6 of the tuple.</typeparam>
    /// <typeparam name="T7">Type of component 7 of the tuple.</typeparam>
    /// <typeparam name="T8">Type of component 8 of the tuple.</typeparam>
    /// <typeparam name="T9">Type of component 9 of the tuple.</typeparam>
    /// <typeparam name="T10">Type of component 10 of the tuple.</typeparam>
    /// <typeparam name="T11">Type of component 11 of the tuple.</typeparam>
    /// <typeparam name="T12">Type of component 12 of the tuple.</typeparam>
    /// <typeparam name="T13">Type of component 13 of the tuple.</typeparam>
    /// <typeparam name="T14">Type of component 14 of the tuple.</typeparam>
    /// <typeparam name="T15">Type of component 15 of the tuple.</typeparam>
    /// <typeparam name="T16">Type of component 16 of the tuple.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct StructTuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> : IDisposable
    {
        /// <summary>
        /// Item number 1 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T1 Item1;

        /// <summary>
        /// Item number 2 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T2 Item2;

        /// <summary>
        /// Item number 3 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T3 Item3;

        /// <summary>
        /// Item number 4 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T4 Item4;

        /// <summary>
        /// Item number 5 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T5 Item5;

        /// <summary>
        /// Item number 6 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T6 Item6;

        /// <summary>
        /// Item number 7 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T7 Item7;

        /// <summary>
        /// Item number 8 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T8 Item8;

        /// <summary>
        /// Item number 9 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T9 Item9;

        /// <summary>
        /// Item number 10 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T10 Item10;

        /// <summary>
        /// Item number 11 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T11 Item11;

        /// <summary>
        /// Item number 12 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T12 Item12;

        /// <summary>
        /// Item number 13 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T13 Item13;

        /// <summary>
        /// Item number 14 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T14 Item14;

        /// <summary>
        /// Item number 15 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T15 Item15;

        /// <summary>
        /// Item number 16 of the tuple object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T16 Item16;

        /// <summary>
        /// Prints a string representation of the StructTuple.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.Item1, this.Item2, this.Item3, this.Item4, this.Item5, this.Item6, this.Item7, this.Item8, this.Item9, this.Item10, this.Item11, this.Item12, this.Item13, this.Item14, this.Item15, this.Item16 }.ToString();

        /// <summary>
        /// Disposes the struct by testing each constituent component for disposability.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
           (this.Item1 as IDisposable)?.Dispose();
           (this.Item2 as IDisposable)?.Dispose();
           (this.Item3 as IDisposable)?.Dispose();
           (this.Item4 as IDisposable)?.Dispose();
           (this.Item5 as IDisposable)?.Dispose();
           (this.Item6 as IDisposable)?.Dispose();
           (this.Item7 as IDisposable)?.Dispose();
           (this.Item8 as IDisposable)?.Dispose();
           (this.Item9 as IDisposable)?.Dispose();
           (this.Item10 as IDisposable)?.Dispose();
           (this.Item11 as IDisposable)?.Dispose();
           (this.Item12 as IDisposable)?.Dispose();
           (this.Item13 as IDisposable)?.Dispose();
           (this.Item14 as IDisposable)?.Dispose();
           (this.Item15 as IDisposable)?.Dispose();
           (this.Item16 as IDisposable)?.Dispose();
        }
    }

}
