// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;

namespace Microsoft.StreamProcessing.Provider
{
    internal class Tests
    {
        private void TestMethod()
        {
            var qc = new QueryContext();
            IObservable<Tuple<string, int, long, long>> obs0 = null;
            IObservable<Tuple<string, int>> obs1 = null;
            IObservable<Tuple<string, int, long>> obs2 = null;

            IQStreamable<Tuple<string, int, long, long>> test0 = qc.RegisterStream(obs0, o => o.Item3, o => o.Item4);
            IQStreamable<Tuple<string, int>> test1 = qc.RegisterStream(obs1, o => 0, o => 0);
            IQStreamable<Tuple<string, int, long>> test2 = qc.RegisterStream(obs2, o => o.Item3, o => o.Item3 + 1);
            var test3 = from t in test1
                        join t1 in test0 on t.Item1 equals t1.Item1
                        join t2 in test2 on t.Item1 equals t2.Item1

                        // join t0 in test0 on t.Item1 equals t0.Item1
                        where t.Item2 < 10
                        let f = t2.Item1
                        group t by t.Item1 into g
                        select g.Window.Count();
        }

        private void TestMethod2()
        {
            IQStreamable<Tuple<string, int, IEnumerable<object>>> test1 = null;
            var test3 = from t in test1
                        from o in t.Item3

                        // join t0 in test0 on t.Item1 equals t0.Item1
                        where t.Item2 < 10
                        group o by t.Item1 into g
                        select g.Window.Count();
        }

    }
}
