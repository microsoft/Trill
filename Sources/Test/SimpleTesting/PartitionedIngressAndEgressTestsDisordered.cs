// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SimpleTesting.PartitionedIngressAndEgress;

namespace SimpleTesting.PartitionedIngressAndEgressDisordered
{
    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermarkNone : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.None());

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.None());
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermark0_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(0, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(0, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermark0_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(0, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(0, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermark0_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(0, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(0, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermark5_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(5, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(5, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermark5_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(5, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(5, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermark5_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(5, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(5, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermark20_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(20, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(20, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermark20_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(20, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(20, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder0LowWatermark20_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(20, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(0), PeriodicLowWatermarkPolicy.Time(20, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermarkNone : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.None());

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.None());
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermark0_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(0, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(0, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermark0_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(0, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(0, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermark0_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(0, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(0, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermark5_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(5, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(5, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermark5_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(5, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(5, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermark5_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(5, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(5, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermark20_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(20, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(20, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermark20_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(20, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(20, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder5LowWatermark20_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(20, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(5), PeriodicLowWatermarkPolicy.Time(20, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermarkNone : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.None());

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.None());
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermark0_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(0, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(0, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermark0_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(0, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(0, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermark0_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(0, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(0, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermark5_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(5, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(5, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermark5_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(5, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(5, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermark5_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(5, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(5, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermark20_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(20, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(20, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermark20_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(20, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(20, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsDropReorder10LowWatermark20_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(20, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Drop(10), PeriodicLowWatermarkPolicy.Time(20, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermarkNone : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.None());

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.None());
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermark0_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(0, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(0, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermark0_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(0, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(0, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermark0_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(0, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(0, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermark5_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(5, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(5, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermark5_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(5, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(5, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermark5_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(5, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(5, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermark20_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(20, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(20, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermark20_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(20, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(20, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder0LowWatermark20_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(20, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(0), PeriodicLowWatermarkPolicy.Time(20, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermarkNone : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.None());

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.None());
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermark0_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(0, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(0, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermark0_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(0, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(0, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermark0_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(0, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(0, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermark5_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(5, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(5, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermark5_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(5, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(5, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermark5_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(5, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(5, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermark20_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(20, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(20, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermark20_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(20, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(20, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder5LowWatermark20_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(20, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(5), PeriodicLowWatermarkPolicy.Time(20, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermarkNone : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.None());

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.None());
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermark0_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(0, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(0, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermark0_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(0, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(0, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermark0_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(0, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(0, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermark5_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(5, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(5, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermark5_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(5, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(5, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermark5_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(5, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(5, 20));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermark20_0 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(20, 0));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(20, 0));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermark20_5 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(20, 5));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(20, 5));
    }

    [TestClass]
    public class TriPartitionedDisorderedTestsAdjustReorder10LowWatermark20_20 : PartitionedDisorderedTestsBase
    {
        [TestMethod, TestCategory("Gated")]
        public void LocalDisordering() => LocalDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(20, 20));

        [TestMethod, TestCategory("Gated")]
        public void ReorderLatencyDisordering() => ReorderLatencyDisorderingBase(DisorderPolicy.Adjust(10), PeriodicLowWatermarkPolicy.Time(20, 20));
    }
}