// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SimpleTesting.PartitionedIngressAndEgress;

namespace SimpleTesting.PartitionedIngressAndEgressOrdered
{
    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow0Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow0Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow10Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow10Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRow1000Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch0Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch10Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsRowSmallBatch1000Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(true).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar0Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar10Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnar1000Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000PunctuationNoneLowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.None(),
            PeriodicLowWatermarkPolicy.None()) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermarkNone : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermarkNone() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark0_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark0_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark0_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark0_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark0_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark0_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark0_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark0_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(0, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark5_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark5_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark5_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark5_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark5_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark5_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark5_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark5_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(5, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark10_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark10_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark10_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark10_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark10_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark10_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark10_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark10_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(10, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark500_0 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark500_0() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 0)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark500_5 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark500_5() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 5)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark500_10 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark500_10() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 10)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch0Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(0),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch10Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(10),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }

    [TestClass]
    public class TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark500_500 : TriPartitionedOrderedTestsBase
    {
        public TriPartitionedOrderedTestsColumnarSmallBatch1000Punctuation10LowWatermark500_500() : base(
            baseConfigModifier.ForceRowBasedExecution(false).DataBatchSize(100),
            DisorderPolicy.Throw(1000),
            PeriodicPunctuationPolicy.Time(10),
            PeriodicLowWatermarkPolicy.Time(500, 500)) { }
    }
}