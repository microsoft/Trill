// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

// Regression coverage for TypeExtensions.CanRepresentAsColumnar. Payload types matching these
// shapes previously reported "columnar-representable" incorrectly: the columnar reconstitution
// codegen assigns each field/autoprop by plain assignment, which an init-only setter, a
// get-only autoprop, or a readonly field cannot accept - such a payload would report
// CanRepresentAsColumnar() == true here and then fail (silently, without
// Config.CodegenOptions.DontFallBackToRowBasedExecution) the first time a query actually tried
// to generate the columnar batch for it.
namespace SimpleTesting.ColumnarTests
{
    [TestClass]
    public class CanRepresentAsColumnarTests
    {
        // NOTE: every fixture below must be a PUBLIC nested type (or Type.IsVisible is false
        // and CanRepresentAsColumnar rejects it for that reason alone, independent of the
        // property/field shape the test actually means to exercise).

        public sealed class MutableClass
        {
            public int A { get; set; }
            public string B { get; set; }
        }

        public sealed class InitOnlyPropertyClass
        {
            public int A { get; init; }
        }

        public sealed class GetOnlyAutoPropertyClass
        {
            public int A { get; }
        }

        public sealed class NonPublicSetterClass
        {
            public int A { get; private set; }
        }

        public sealed class ManualBackingFieldClass
        {
            private int _a;
            public int A { get => this._a; set => this._a = value; }
        }

        public abstract class AbstractClass
        {
            public int A { get; set; }
        }

        public sealed class NoNullaryCtorClass
        {
            public NoNullaryCtorClass(int a) => this.A = a;
            public int A { get; set; }
        }

        // Positional record class: the primary constructor is the only one, so there is no
        // nullary ctor. This one was already correctly rejected before the fix - included as a
        // non-regression check.
        public sealed record PositionalRecordClass(int A, string B);

        public record struct MutableRecordStruct(int A, string B);

        // A positional record struct's auto-generated properties are init-only.
        public readonly record struct ReadonlyRecordStruct(int A, string B);

        public struct MutableFieldStruct
        {
            public int A;

            public MutableFieldStruct(int a) => this.A = a;
        }

        public struct ReadonlyFieldStruct
        {
            public readonly int A;
            public ReadonlyFieldStruct(int a) => this.A = a;
        }

        public struct InitOnlyPropertyStruct
        {
            public int A { get; init; }
        }

        // Every `record` (not `record struct`) synthesizes a compiler-generated, get-only,
        // non-public `EqualityContract` property. A plain mutable record class must still be
        // columnar-representable despite that - EqualityContract has no independent storage and
        // is not row state.
        public sealed record PlainRecordClass
        {
            public int A { get; set; }
            public string B { get; set; } = "";
        }

        // A genuinely non-public autoprop with BOTH accessors present (as opposed to
        // EqualityContract's get-only shape) must still disqualify the type - the
        // EqualityContract carve-out is narrowly for "non-public and no setter at all".
        public sealed class NonPublicBothAccessorsClass
        {
            internal int Hidden { get; set; }

            public int Exposed { get => this.Hidden; set => this.Hidden = value; }
        }

        [TestMethod]
        public void MutableClass_IsColumnarRepresentable() =>
            Assert.IsTrue(typeof(MutableClass).CanRepresentAsColumnar());

        [TestMethod]
        public void InitOnlyProperty_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(InitOnlyPropertyClass).CanRepresentAsColumnar());

        [TestMethod]
        public void GetOnlyAutoProperty_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(GetOnlyAutoPropertyClass).CanRepresentAsColumnar());

        [TestMethod]
        public void NonPublicSetter_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(NonPublicSetterClass).CanRepresentAsColumnar());

        [TestMethod]
        public void ManualBackingField_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(ManualBackingFieldClass).CanRepresentAsColumnar());

        [TestMethod]
        public void AbstractClass_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(AbstractClass).CanRepresentAsColumnar());

        [TestMethod]
        public void NoNullaryCtor_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(NoNullaryCtorClass).CanRepresentAsColumnar());

        [TestMethod]
        public void PositionalRecordClass_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(PositionalRecordClass).CanRepresentAsColumnar());

        [TestMethod]
        public void MutableRecordStruct_IsColumnarRepresentable() =>
            Assert.IsTrue(typeof(MutableRecordStruct).CanRepresentAsColumnar());

        [TestMethod]
        public void ReadonlyRecordStruct_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(ReadonlyRecordStruct).CanRepresentAsColumnar());

        [TestMethod]
        public void MutableFieldStruct_IsColumnarRepresentable() =>
            Assert.IsTrue(typeof(MutableFieldStruct).CanRepresentAsColumnar());

        [TestMethod]
        public void ReadonlyFieldStruct_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(ReadonlyFieldStruct).CanRepresentAsColumnar());

        [TestMethod]
        public void InitOnlyPropertyStruct_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(InitOnlyPropertyStruct).CanRepresentAsColumnar());

        [TestMethod]
        public void Primitive_IsColumnarRepresentable() =>
            Assert.IsTrue(typeof(int).CanRepresentAsColumnar());

        [TestMethod]
        public void PlainRecordClass_IsColumnarRepresentable_DespiteSynthesizedEqualityContract() =>
            Assert.IsTrue(typeof(PlainRecordClass).CanRepresentAsColumnar());

        [TestMethod]
        public void NonPublicAutopropWithBothAccessors_IsNotColumnarRepresentable() =>
            Assert.IsFalse(typeof(NonPublicBothAccessorsClass).CanRepresentAsColumnar());
    }
}
