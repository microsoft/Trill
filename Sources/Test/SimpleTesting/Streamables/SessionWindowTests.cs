// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class SessionWindowTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SessionWindowTests()
            : base(new ConfigModifier().ForceRowBasedExecution(true))
        {
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionWindowLifetimeTest()
        {
            const long sessionDuration = 10;
            var input = new Subject<StreamEvent<string>>();

            var qc = new QueryContainer();
            var inputStream = qc.RegisterInput(input);
            var query = inputStream.SessionWindow(sessionDuration);

            var output = new List<StreamEvent<string>>();
            qc.RegisterOutput(query).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();

            // Output should be available immediately
            input.OnNext(StreamEvent.CreatePoint(1, "A"));
            input.OnNext(StreamEvent.CreatePoint(2, "B"));
            input.OnNext(StreamEvent.CreatePoint(3, "C"));
            process.Flush();
            Assert.AreEqual(3, output.Count);
            Assert.AreEqual(output[0], StreamEvent.CreateInterval(1, 11, "A"));
            Assert.AreEqual(output[1], StreamEvent.CreateInterval(2, 11, "B"));
            Assert.AreEqual(output[2], StreamEvent.CreateInterval(3, 11, "C"));
            output.Clear();

            // Three different sessions
            input.OnNext(StreamEvent.CreatePoint(10, "D"));
            input.OnNext(StreamEvent.CreatePoint(11, "E"));
            input.OnNext(StreamEvent.CreatePoint(22, "F"));
            process.Flush();
            Assert.AreEqual(3, output.Count);
            Assert.AreEqual(output[0], StreamEvent.CreateInterval(10, 11, "D"));
            Assert.AreEqual(output[1], StreamEvent.CreateInterval(11, 21, "E"));
            Assert.AreEqual(output[2], StreamEvent.CreateInterval(22, 32, "F"));

            input.OnCompleted();
        }
    }
}