// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Diagnostics;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class AbstractStreamMessage : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public AbstractStreamMessage()
            : base(new ConfigModifier().DataBatchSize(80000)) // Note: These tests are sensitive to the exact value of databatchsize.
        { }

        // Set bit at idx.
        private static void VacateAt<T>(StreamMessage<T, bool> tm, int idx)
            => tm.bitvector.col[idx >> 6] = tm.bitvector.col[idx >> 6] | (1L << (idx & 0x3f));

        // Reset bit at idx.
        private static void OccupyAt<T>(StreamMessage<T, bool> tm, int idx)
            => tm.bitvector.col[idx >> 6] = tm.bitvector.col[idx >> 6] & ~(1L << (idx & 0x3f));

        [TestMethod, TestCategory("Gated")]
        public void ComputeMinMax_Basic()
        {
            const long TESTVALUE = 635490785890000000;
            var memoryPool = MemoryManager.GetMemoryPool<Empty, bool>(true);
            var message = new StreamMessage<Empty, bool>(memoryPool);
            message.Allocate();

            // mark all positions as not occupied
            for (int i = 0; i < (Config.DataBatchSize >> 6); i++) message.bitvector.col[i] = ~0L;

            // Implements following checks:
            // a) Count = 0                     ==> min = 0,      max = inf-1
            // b) Count = 1, pos 0 not occupied ==> min = 0,      max = inf-1
            // c) Count = 1, pos 0 occupied     ==> min = pos0val,max = pos0val
            // d) Count > 1, nothing occupied   ==> min = 0,      max = inf-1
            // e) Count > 1, some occupied      ==> min = minval, max = maxval

            // (a)
            message.Count = 0;
            Assert.IsTrue(message.MinTimestamp == StreamEvent.MinSyncTime, "Empty message with Count=0 has MinTimestamp <> 0.");
            Assert.IsTrue(message.MaxTimestamp == StreamEvent.MaxSyncTime, "Empty message with Count=0 has MaxTimestamp <> inf-1.");

            // (b)
            message.Count = 1;
            message.vsync.col[0] = TESTVALUE;
            Assert.IsTrue(message.MinTimestamp == StreamEvent.MinSyncTime, "Empty message with Count=1 has MinTimestamp <> 0.");
            Assert.IsTrue(message.MaxTimestamp == StreamEvent.MaxSyncTime, "Empty message with Count=1 has MaxTimestamp <> inf-1.");

            // (c)
            message.Count = 1;
            message.vsync.col[0] = TESTVALUE;
            OccupyAt(message, 0);
            Assert.IsTrue(message.MinTimestamp == TESTVALUE, "Empty message with Count=1 has MinTimestamp <> TESTVALUE.");
            Assert.IsTrue(message.MaxTimestamp == TESTVALUE, "Empty message with Count=1 has MaxTimestamp <> TESTVALUE");
            VacateAt(message, 0);
            message.vsync.col[0] = 0;

            // (d)
            message.Count = 100;
            message.vsync.col[5] = TESTVALUE - 1;
            message.vsync.col[42] = TESTVALUE;
            Assert.IsTrue(message.MinTimestamp == StreamEvent.MinSyncTime, "Empty message with Count>1 has MinTimestamp <> 0.");
            Assert.IsTrue(message.MaxTimestamp == StreamEvent.MaxSyncTime, "Empty message with Count>1 has MaxTimestamp <> inf-1");

            // (e)
            OccupyAt(message, 5);
            OccupyAt(message, 42);
            Assert.IsTrue(message.MinTimestamp == TESTVALUE - 1, "Non-empty message with Count>1 has MinTimestamp <> TESTVALUE-1.");
            Assert.IsTrue(message.MaxTimestamp == TESTVALUE, "Non-empty message with Count>1 has MaxTimestamp <> TESTVALUE");
            VacateAt(message, 5);
            VacateAt(message, 42);
            message.vsync.col[5] = 0;
            message.vsync.col[42] = 0;

            message.Release();
        }

        [TestMethod, TestCategory("Gated")]
        public void ComputeMinMax()
        {
            bool isPassed = true;

            const long TESTVALUE = 635490785890000000;
            var memoryPool = MemoryManager.GetMemoryPool<Empty, bool>(true);
            var message = new StreamMessage<Empty, bool>(memoryPool);
            message.Allocate();

            // mark all positions as not occupied
            for (int i = 0; i < (Config.DataBatchSize >> 6); i++) message.bitvector.col[i] = ~0L;

            // Single event
            for (var position = 0; position < 1000; position++)
            {
                message.Count = position + 1;

                OccupyAt(message, position);
                message.vsync.col[position] = TESTVALUE;

                bool check1, check2;
                if (!(check1 = message.MinTimestamp == TESTVALUE))
                    Debug.WriteLine("FAILED: Single value in position {0} for MinTimestamp.", position);
                if (!(check2 = message.MaxTimestamp == TESTVALUE))
                    Debug.WriteLine("FAILED: Single value in position {0} for MaxTimestamp.", position);
                isPassed = isPassed && check1 && check2;

                VacateAt(message, position);
                message.vsync.col[position] = 0;
            }

            // Two consecutive events, somewhat in the middle
            for (var pos = 0; pos < Config.DataBatchSize / 2; pos++)
            {
                message.Count = pos + 2;
                var position = pos >> 1;

                OccupyAt(message, position);
                OccupyAt(message, position + 1);
                message.vsync.col[position] = TESTVALUE;
                message.vsync.col[position + 1] = TESTVALUE + 1;

                bool check1, check2;
                if (!(check1 = message.MinTimestamp == TESTVALUE))
                    Debug.WriteLine("FAILED: Two values in positions {0}, {1} for MinTimestamp.", position, position + 1);
                if (!(check2 = message.MaxTimestamp == TESTVALUE + 1))
                    Debug.WriteLine("FAILED: Two values in positions {0}, {1} for MaxTimestamp.", position, position + 1);
                isPassed = isPassed && check1 && check2;

                VacateAt(message, position);
                VacateAt(message, position + 1);
                message.vsync.col[position] = 0;
                message.vsync.col[position + 1] = 0;
            }

            Assert.IsTrue(isPassed, "Failed - see output for details.");
            message.Release();
        }

        [TestMethod, TestCategory("Gated")]
        public void ComputeMinMax_Partitioned_Basic()
        {
            const long TESTVALUE = 635490785890000000;
            var memoryPool = MemoryManager.GetMemoryPool<PartitionKey<bool>, bool>(true);
            var message = new StreamMessage<PartitionKey<bool>, bool>(memoryPool);
            message.Allocate();

            // mark all positions as not occupied
            for (int i = 0; i < (Config.DataBatchSize >> 6); i++) message.bitvector.col[i] = ~0L;

            // Implements following checks:
            // a) Count = 0                     ==> min = 0,      max = inf-1
            // b) Count = 1, pos 0 not occupied ==> min = 0,      max = inf-1
            // c) Count = 1, pos 0 occupied     ==> min = pos0val,max = pos0val
            // d) Count > 1, nothing occupied   ==> min = 0,      max = inf-1
            // e) Count > 1, some occupied      ==> min = minval, max = maxval

            // (a)
            message.Count = 0;
            Assert.IsTrue(message.MinTimestamp == StreamEvent.MinSyncTime, "Empty message with Count=0 has MinTimestamp <> 0.");
            Assert.IsTrue(message.MaxTimestamp == StreamEvent.MaxSyncTime, "Empty message with Count=0 has MaxTimestamp <> inf-1.");

            // (b)
            message.Count = 1;
            message.vsync.col[0] = TESTVALUE;
            Assert.IsTrue(message.MinTimestamp == StreamEvent.MinSyncTime, "Empty message with Count=1 has MinTimestamp <> 0.");
            Assert.IsTrue(message.MaxTimestamp == StreamEvent.MaxSyncTime, "Empty message with Count=1 has MaxTimestamp <> inf-1.");

            // (c)
            message.Count = 1;
            message.vsync.col[0] = TESTVALUE;
            OccupyAt(message, 0);
            Assert.IsTrue(message.MinTimestamp == TESTVALUE, "Empty message with Count=1 has MinTimestamp <> TESTVALUE.");
            Assert.IsTrue(message.MaxTimestamp == TESTVALUE, "Empty message with Count=1 has MaxTimestamp <> TESTVALUE");
            VacateAt(message, 0);
            message.vsync.col[0] = 0;

            // (d)
            message.Count = 100;
            message.vsync.col[5] = TESTVALUE - 1;
            message.vsync.col[42] = TESTVALUE;
            Assert.IsTrue(message.MinTimestamp == StreamEvent.MinSyncTime, "Empty message with Count>1 has MinTimestamp <> 0.");
            Assert.IsTrue(message.MaxTimestamp == StreamEvent.MaxSyncTime, "Empty message with Count>1 has MaxTimestamp <> inf-1");

            // (e)
            OccupyAt(message, 5);
            OccupyAt(message, 42);
            Assert.IsTrue(message.MinTimestamp == TESTVALUE - 1, "Non-empty message with Count>1 has MinTimestamp <> TESTVALUE-1.");
            Assert.IsTrue(message.MaxTimestamp == TESTVALUE, "Non-empty message with Count>1 has MaxTimestamp <> TESTVALUE");
            VacateAt(message, 5);
            VacateAt(message, 42);
            message.vsync.col[5] = 0;
            message.vsync.col[42] = 0;

            message.Release();
        }

        [TestMethod, TestCategory("Gated")]
        public void ComputeMinMax_Partitioned_Compound_Basic()
        {
            const long TESTVALUE = 635490785890000000;
            var memoryPool = MemoryManager.GetMemoryPool<CompoundGroupKey<PartitionKey<bool>, bool>, bool>(true);
            var message = new StreamMessage<CompoundGroupKey<PartitionKey<bool>, bool>, bool>(memoryPool);
            message.Allocate();

            // mark all positions as not occupied
            for (int i = 0; i < (Config.DataBatchSize >> 6); i++) message.bitvector.col[i] = ~0L;

            // Implements following checks from the Basic test, but for CompoundGroupKey case:
            // d) Count > 1, nothing occupied   ==> min = 0,      max = inf-1
            // e) Count > 1, some occupied      ==> min = minval, max = maxval

            // (d)
            message.Count = 100;
            message.vsync.col[5] = TESTVALUE;
            message.vsync.col[42] = TESTVALUE - 1;
            Assert.IsTrue(message.MinTimestamp == StreamEvent.MinSyncTime, "Empty message with Count>1 has MinTimestamp <> 0.");
            Assert.IsTrue(message.MaxTimestamp == StreamEvent.MaxSyncTime, "Empty message with Count>1 has MaxTimestamp <> inf-1");

            // (e)
            OccupyAt(message, 5);
            OccupyAt(message, 42);
            Assert.IsTrue(message.MinTimestamp == TESTVALUE - 1, "Non-empty message with Count>1 has MinTimestamp <> TESTVALUE-1.");
            Assert.IsTrue(message.MaxTimestamp == TESTVALUE, "Non-empty message with Count>1 has MaxTimestamp <> TESTVALUE");
            VacateAt(message, 5);
            VacateAt(message, 42);
            message.vsync.col[5] = 0;
            message.vsync.col[42] = 0;

            message.Release();
        }

        [TestMethod, TestCategory("Gated")]
        public void ComputeMinMax_Partitioned()
        {
            bool isPassed = true;

            const long TESTVALUE = 635490785890000000;
            var memoryPool = MemoryManager.GetMemoryPool<PartitionKey<bool>, bool>(true);
            var message = new StreamMessage<PartitionKey<bool>, bool>(memoryPool);
            message.Allocate();

            // mark all positions as not occupied
            for (int i = 0; i < (Config.DataBatchSize >> 6); i++) message.bitvector.col[i] = ~0L;

            // Single event
            for (var position = 0; position < 1000; position++)
            {
                message.Count = position + 1;

                OccupyAt(message, position);
                message.vsync.col[position] = TESTVALUE;

                bool check1, check2;
                if (!(check1 = message.MinTimestamp == TESTVALUE))
                    Debug.WriteLine("FAILED: Single value in position {0} for MinTimestamp.", position);
                if (!(check2 = message.MaxTimestamp == TESTVALUE))
                    Debug.WriteLine("FAILED: Single value in position {0} for MaxTimestamp.", position);
                isPassed = isPassed && check1 && check2;

                VacateAt(message, position);
                message.vsync.col[position] = 0;
            }

            // Two consecutive events, somewhat in the middle
            for (var pos = 0; pos < Config.DataBatchSize / 2; pos++)
            {
                message.Count = pos + 2;
                var position = pos >> 1;

                OccupyAt(message, position);
                OccupyAt(message, position + 1);
                if (pos % 2 == 0)
                {
                    message.vsync.col[position] = TESTVALUE;
                    message.vsync.col[position + 1] = TESTVALUE + 1;
                }
                else
                {
                    message.vsync.col[position] = TESTVALUE + 1;
                    message.vsync.col[position + 1] = TESTVALUE;
                }
                bool check1, check2;
                if (!(check1 = message.MinTimestamp == TESTVALUE))
                    Debug.WriteLine("FAILED: Two values in positions {0}, {1} for MinTimestamp.", position, position + 1);
                if (!(check2 = message.MaxTimestamp == TESTVALUE + 1))
                    Debug.WriteLine("FAILED: Two values in positions {0}, {1} for MaxTimestamp.", position, position + 1);
                isPassed = isPassed && check1 && check2;

                VacateAt(message, position);
                VacateAt(message, position + 1);
                message.vsync.col[position] = 0;
                message.vsync.col[position + 1] = 0;
            }

            Assert.IsTrue(isPassed, "Failed - see output for details.");
            message.Release();
        }

        [TestMethod, TestCategory("Gated")]
        public void ComputeCount()
        {
            var memoryPool = MemoryManager.GetMemoryPool<Empty, bool>(true);
            var message = new StreamMessage<Empty, bool>(memoryPool);
            message.Allocate();

            // Tests with the bit vector set to all 0s
            message.Count = 0;
            Assert.AreEqual(message.Count, message.ComputeCount());

            message.Count = 52; // Smaller than 64 so we have to hand count
            Assert.AreEqual(message.Count, message.ComputeCount());

            message.Count = 1200 * 64; // A multiple of 64
            Assert.AreEqual(message.Count, message.ComputeCount());

            message.Count = (1200 * 64) + 27; // Not a multiple of 64
            Assert.AreEqual(message.Count, message.ComputeCount());

            // Tests with the bit vector that has some of its value set to 1
            message.bitvector.col[0] = 0X100F000000000000;
            message.bitvector.col[500] = 0xF0E0;
            message.bitvector.col[1199] = 0x1000000000000000;
            message.bitvector.col[1200] = 0x31000000;

            message.Count = 0;
            Assert.AreEqual(message.Count, message.ComputeCount()); // Bit of a red herring since the bit vector is to be ignored when count == 0

            message.Count = 52;
            Assert.AreEqual(message.Count - 4, message.ComputeCount()); // Only 4 of the bits should be 'visible' with a count of 52

            message.Count = 1200 * 64;
            Assert.AreEqual(message.Count - 13, message.ComputeCount());

            message.Count = (1200 * 64) + 27;
            Assert.AreEqual(message.Count - 14, message.ComputeCount());

            // Test ranges
            message.Count = 64;
            Assert.AreEqual(1, message.ComputeCount(59, 60));
            Assert.AreEqual(0, message.ComputeCount(60, 60));
            Assert.AreEqual(1, message.ComputeCount(60, 61));

            message.Count = 1200 * 64;
            Assert.AreEqual(7 - 7 + 1 - 1, message.ComputeCount((500 * 64) + 7, (500 * 64) + 7));
            Assert.AreEqual(10 - 4 + 1 - 3, message.ComputeCount((500 * 64) + 4, (500 * 64) + 10));
            Assert.AreEqual((500 * 64) + 7 - 61 + 1 - 3, message.ComputeCount(61, (500 * 64) + 7));
            Assert.AreEqual((500 * 64) + 8 - 60 + 1 - 4, message.ComputeCount(60, (500 * 64) + 8));

            // Test high order bits
            message.bitvector.col[1210] = 0x5FFFFFFFFFFFFFFF;
            message.Count = (1210 * 64) + 100;
            Assert.AreEqual(0, message.ComputeCount(1210 << 6, 1210 << 6));
            Assert.AreEqual(1, message.ComputeCount((1210 << 6) + 61, (1210 << 6) + 61));
            Assert.AreEqual(2, message.ComputeCount(1210 << 6, (1210 << 6) + 63));
            Assert.AreEqual(2 + 2, message.ComputeCount((1210 << 6) - 1, (1210 << 6) + 64));
            message.Release();
        }
    }
}
