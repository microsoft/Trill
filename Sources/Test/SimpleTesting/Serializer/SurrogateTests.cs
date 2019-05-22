// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reflection;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Serializer;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public class MySurrogate : ISurrogate
    {
        public bool IsSupportedType(Type t, out MethodInfo serialize, out MethodInfo deserialize)
        {
            serialize = null;
            deserialize = null;

            if (t == typeof(IMyInterface))
            {
                serialize = typeof(MySurrogate).GetMethod("Serialize");
                deserialize = typeof(MySurrogate).GetMethod("Deserialize");
                return true;
            }

            return false;
        }

        public void Serialize(IMyInterface obj, Stream stream)
        {
            var m = (MyType)obj;
            using (var w = new BinaryWriter(stream, System.Text.Encoding.UTF8, true))
            {
                if (obj != null)
                {
                    w.Write((byte)1);
                    w.Write(m.GetValue());
                }
                else
                {
                    w.Write((byte)0);
                }
            }
        }

        public IMyInterface Deserialize(Stream stream)
        {
            using (var w = new BinaryReader(stream, System.Text.Encoding.UTF8, true))
            {
                var isNotNull = w.ReadByte();
                return isNotNull == 1 ? new MyType(w.ReadSingle()) : null;
            }
        }
    }

    public interface IMyInterface
    {
        float GetValue();
    }

    public class MyType : IMyInterface
    {
        private readonly float value = 0;
        public MyType(float value) => this.value = value;

        public float GetValue() => this.value;
    }

    [TestClass]
    public class SurrogateTests : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public SurrogateTests() : base(new ConfigModifier()
            .ForceRowBasedExecution(true))
        { }

        [TestMethod, TestCategory("Gated")]
        public void SurrogateTest()
        {
            var qc = new QueryContainer(new MySurrogate());

            var output1 = new List<StreamEvent<IMyInterface>>();
            var input = new Subject<StreamEvent<IMyInterface>>();

            var ingress = qc.RegisterInput(input);
            var egress = qc.RegisterOutput(ingress).ForEachAsync(o => output1.Add(o));
            var process = qc.Restore();

            input.OnNext(StreamEvent.CreatePoint(1, (IMyInterface)new MyType(1)));

            var stream = new MemoryStream();
            process.Checkpoint(stream);
            stream.Position = 0;

            input.OnCompleted();

            var input2 = new Subject<StreamEvent<IMyInterface>>();
            var output2 = new List<StreamEvent<IMyInterface>>();

            var qc2 = new QueryContainer(new MySurrogate());
            var ingress2 = qc2.RegisterInput(input2);
            var egress2 = qc2.RegisterOutput(ingress2).ForEachAsync(o => output2.Add(o));
            var process2 = qc2.Restore(stream);

            input2.OnCompleted();

            Assert.AreEqual(2, output2.Count);
            Assert.AreEqual(1, output2[0].Payload.GetValue());
            Assert.AreEqual(StreamEvent.InfinitySyncTime, output2[1].SyncTime);
        }
    }
}