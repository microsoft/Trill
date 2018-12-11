// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting.Flush
{
    [TestClass]
    public class FlushNone_OnCompletedNone : FlushTestBase
    {
        public FlushNone_OnCompletedNone() : base(DisorderPolicy.Throw(), FlushPolicy.None, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None)
        { }
    }

    [TestClass]
    public class FlushOnPunctuation_OnCompletedNone : FlushTestBase
    {
        public FlushOnPunctuation_OnCompletedNone() : base(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None)
        { }
    }

    [TestClass]
    public class FlushOnBatchBoundary_OnCompletedNone : FlushTestBase
    {
        public FlushOnBatchBoundary_OnCompletedNone() : base(DisorderPolicy.Throw(), FlushPolicy.FlushOnBatchBoundary, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None)
        { }
    }

    [TestClass]
    public class FlushNone_OnCompletedFlush : FlushTestBase
    {
        public FlushNone_OnCompletedFlush() : base(DisorderPolicy.Throw(), FlushPolicy.None, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.Flush)
        { }
    }

    [TestClass]
    public class FlushOnPunctuation_OnCompletedFlush : FlushTestBase
    {
        public FlushOnPunctuation_OnCompletedFlush() : base(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.Flush)
        { }
    }

    [TestClass]
    public class FlushOnBatchBoundary_OnCompletedFlush : FlushTestBase
    {
        public FlushOnBatchBoundary_OnCompletedFlush() : base(DisorderPolicy.Throw(), FlushPolicy.FlushOnBatchBoundary, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.Flush)
        { }
    }

    [TestClass]
    public class FlushNone_OnCompletedEndOfStream : FlushTestBase
    {
        public FlushNone_OnCompletedEndOfStream() : base(DisorderPolicy.Throw(), FlushPolicy.None, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.EndOfStream)
        { }
    }

    [TestClass]
    public class FlushOnPunctuation_OnCompletedEndOfStream : FlushTestBase
    {
        public FlushOnPunctuation_OnCompletedEndOfStream() : base(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.EndOfStream)
        { }
    }

    [TestClass]
    public class FlushOnBatchBoundary_OnCompletedEndOfStream : FlushTestBase
    {
        public FlushOnBatchBoundary_OnCompletedEndOfStream() : base(DisorderPolicy.Throw(), FlushPolicy.FlushOnBatchBoundary, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.EndOfStream)
        { }
    }
}