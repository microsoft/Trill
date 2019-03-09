// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal static class CallInliner
    {
        public static int Call<TComparison>(Expression<Comparison<TComparison>> expression, TComparison input1, TComparison input2)
            => throw new NotImplementedException();

        public static TOutput Call<TOutput>(Expression<Func<TOutput>> expression)
            => throw new NotImplementedException();

        public static TOutput Call<T1, TOutput>(Expression<Func<T1, TOutput>> expression, T1 input1)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, TOutput>(Expression<Func<T1, T2, TOutput>> expression, T1 input1, T2 input2)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, TOutput>(Expression<Func<T1, T2, T3, TOutput>> expression, T1 input1, T2 input2, T3 input3)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, TOutput>(Expression<Func<T1, T2, T3, T4, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, TOutput>(Expression<Func<T1, T2, T3, T4, T5, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, T8, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, T8, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7, T8 input8)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, T8, T9, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7, T8 input8, T9 input9)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7, T8 input8, T9 input9, T10 input10)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7, T8 input8, T9 input9, T10 input10, T11 input11)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7, T8 input8, T9 input9, T10 input10, T11 input11, T12 input12)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7, T8 input8, T9 input9, T10 input10, T11 input11, T12 input12, T13 input13)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7, T8 input8, T9 input9, T10 input10, T11 input11, T12 input12, T13 input13, T14 input14)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7, T8 input8, T9 input9, T10 input10, T11 input11, T12 input12, T13 input13, T14 input14, T15 input15)
            => throw new NotImplementedException();

        public static TOutput Call<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TOutput>(Expression<Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TOutput>> expression, T1 input1, T2 input2, T3 input3, T4 input4, T5 input5, T6 input6, T7 input7, T8 input8, T9 input9, T10 input10, T11 input11, T12 input12, T13 input13, T14 input14, T15 input15, T16 input16)
            => throw new NotImplementedException();
    }

}