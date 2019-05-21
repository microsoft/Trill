// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Reflection;
using System.Threading;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Choices for what technique should be used at data ingress for sorting any out-of-order data.
    /// </summary>
    public enum SortingTechnique
    {
        /// <summary>
        /// Specifies that the query processor should use a priority queue to sort out-of-order data.
        /// </summary>
        PriorityQueue,

        /// <summary>
        /// Specifies that the query processor should use the impatience sort method to sort out-of-order data.
        /// </summary>
        ImpatienceSort
    }

    [Flags]
    internal enum SerializationCompressionLevel
    {
        None = 0,
        CharArrayToUTF8 = 1,
    }

    /// <summary>
    /// Static class that holds all user configuration settings.
    /// </summary>
    public static class Config
    {
        private static bool forceRowBasedExecution = false;
        private static bool allowFloatingReorderPolicy = false;

        private static bool clearColumnsOnReturn = false;
        private static bool disableMemoryPooling = false;
        private static int dataBatchSize = 80000;
        private static bool useMultiString = false;
        private static SortingTechnique ingressSortingTechnique = SortingTechnique.ImpatienceSort;
        private static CodegenOptions.MultiStringFlags useMultiStringTransforms = CodegenOptions.MultiStringFlags.None;
        private static StreamScheduler streamScheduler = StreamScheduler.Null();
        private static bool deterministicWithinTimestamp = true;
        private static SerializationCompressionLevel serializationCompressionLevel = SerializationCompressionLevel.None;
        private static int aggregateHashTableInitSize = 1;
        private static string generatedCodePath = "Generated";

        /// <summary>
        /// The file system location to which any generated code artifacts should be stored.
        /// </summary>
        public static string GeneratedCodePath
        {
            get => generatedCodePath;
            set
            {
                TraceConfigChanges("GeneratedCodePath", generatedCodePath, value);
                generatedCodePath = value;
            }
        }

        /// <summary>
        /// Number of mapper threads created by map-reduce. One implies no thread creation (work on user thread).
        /// </summary>
        internal static int MapArity
        {
            get => streamScheduler.scheduler.MapArity;
            set
            {
                if (value == 1 && (streamScheduler.scheduler.ReduceArity == 1))
                {
                    if ((streamScheduler.scheduler as NullScheduler) == null)
                    {
                        var newScheduler = StreamScheduler.Null();
                        TraceConfigChanges("MapArity", streamScheduler.scheduler.MapArity, newScheduler.scheduler.MapArity);
                        streamScheduler = newScheduler;
                    }
                }
                else
                {
                    var newScheduler = StreamScheduler.OwnedThreads(value);
                    TraceConfigChanges("MapArity", streamScheduler.scheduler.MapArity, newScheduler.scheduler.MapArity);
                    streamScheduler = newScheduler;
                }
            }
        }

        /// <summary>
        /// Number of reducer threads created by map-reduce. One implies no thread creation (work on user thread).
        /// </summary>
        internal static int ReduceArity
        {
            get => streamScheduler.scheduler.ReduceArity;
            set
            {
                if (value == 1 && (streamScheduler.scheduler.MapArity == 1))
                {
                    if ((streamScheduler.scheduler as NullScheduler) == null)
                    {
                        var newScheduler = StreamScheduler.Null();
                        TraceConfigChanges("ReduceArity", streamScheduler.scheduler.ReduceArity, newScheduler.scheduler.ReduceArity);
                        streamScheduler = newScheduler;
                    }
                }
                else
                {
                    var newScheduler = StreamScheduler.OwnedThreads(value);
                    TraceConfigChanges("ReduceArity", streamScheduler.scheduler.ReduceArity, newScheduler.scheduler.ReduceArity);
                    streamScheduler = newScheduler;
                }
            }
        }

        /// <summary>
        /// Use the row-based implementation of every operator. No code generation.
        /// </summary>
        public static bool ForceRowBasedExecution
        {
            get => forceRowBasedExecution;
            set
            {
                TraceConfigChanges("ForceRowBasedExecution", forceRowBasedExecution, value);
                forceRowBasedExecution = value;
            }
        }

        /// <summary>
        /// Allow the reorder policy to be applied later in the query than the ingress site, or possibly not at all if it is not required by the query.
        /// </summary>
        public static bool AllowFloatingReorderPolicy
        {
            get => allowFloatingReorderPolicy;
            set
            {
                TraceConfigChanges("AllowFloatingReorderPolicy", allowFloatingReorderPolicy, value);
                allowFloatingReorderPolicy = value;
            }
        }

        /// <summary>
        /// Multi-input operators are made deterministic within the same timestamp. Currently only done for union.
        /// </summary>
        public static bool DeterministicWithinTimestamp
        {
            get => deterministicWithinTimestamp;
            set
            {
                TraceConfigChanges("DeterministicWithinTimestamp", deterministicWithinTimestamp, value);
                deterministicWithinTimestamp = value;
            }
        }

        /// <summary>
        /// Clear (zero out) columns before returning them to a pool
        /// </summary>
        public static bool ClearColumnsOnReturn
        {
            get => clearColumnsOnReturn;
            set
            {
                TraceConfigChanges("ClearColumnsOnReturn", clearColumnsOnReturn, value);
                clearColumnsOnReturn = value;
            }
        }

        /// <summary>
        /// Disable Trill's memory pooling functionality
        /// </summary>
        public static bool DisableMemoryPooling
        {
            get => disableMemoryPooling;
            set
            {
                TraceConfigChanges("DisableMemoryPooling", disableMemoryPooling, value);
                disableMemoryPooling = value;
            }
        }

        /// <summary>
        /// Maximum number of tuples in each DataBatch message.
        /// </summary>
        public static int DataBatchSize
        {
            get => dataBatchSize;
            set
            {
                // Needs to be non-zero
                Contract.Requires(value > 0);

                TraceConfigChanges("DataBatchSize", dataBatchSize, value);
                dataBatchSize = value;
            }
        }

        /// <summary>
        /// Optimize strings in generated batches.
        /// </summary>
        internal static bool UseMultiString
        {
            get => useMultiString;
            set
            {
                TraceConfigChanges("UseMultiString", useMultiString, value);
                useMultiString = value;
            }
        }

        /// <summary>
        /// Choose sorting technique to use an ingress
        /// </summary>
        public static SortingTechnique IngressSortingTechnique
        {
            get => ingressSortingTechnique;
            set
            {
                TraceConfigChanges("IngressSortingTechnique", ingressSortingTechnique, value);
                ingressSortingTechnique = value;
            }
        }

        /// <summary>
        /// Whenever possible, transform method calls on strings into operations on MultiStrings.
        /// In effect only if UseMultiString is true.
        /// </summary>
        internal static CodegenOptions.MultiStringFlags MultiStringTransforms
        {
            get => useMultiStringTransforms;
            set
            {
                TraceConfigChanges("MultiStringTransforms", useMultiStringTransforms, value);
                useMultiStringTransforms = value;
            }
        }

        /// <summary>
        /// Change the scheduler used to run Trill using multiple cores.
        /// </summary>
        public static StreamScheduler StreamScheduler
        {
            get => streamScheduler;
            set
            {
                TraceConfigChanges("Scheduler", SchedToStr(streamScheduler.scheduler), SchedToStr(value?.scheduler));
                streamScheduler = value;
            }
        }

        /// <summary>
        /// What size to initialize hash tables to.
        /// </summary>
        internal static int AggregateHashTableInitSize
        {
            get => aggregateHashTableInitSize;
            set
            {
                TraceConfigChanges("AggregateHashTableInitSize", aggregateHashTableInitSize, value);
                aggregateHashTableInitSize = value;
            }
        }

        /// <summary>
        /// Compression level used in serialization.
        /// </summary>
        internal static SerializationCompressionLevel SerializationCompressionLevel
        {
            get => serializationCompressionLevel;
            set
            {
                TraceConfigChanges("SerializationCompressionLevel", serializationCompressionLevel, value);
                serializationCompressionLevel = value;
            }
        }

        /// <summary>
        /// A place to organize all codegen related options
        /// </summary>
        internal static class CodegenOptions
        {
#if DEBUG
            private static bool generateDebugInfo = true;
#else
            private static bool generateDebugInfo = false;
#endif
            private static DebugFlags breakIntoCodeGen = DebugFlags.None;
            private static bool dontFallBackToRowBasedExecution = false;
            private static bool superStrictColumnar = false;
            private static bool codeGenAfa = true;

            /// <summary>
            /// When true then generate debugging information for the generated code.
            /// The default for Debug builds of Trill is to generate debugging information.
            /// The default for Release builds of Trill is not generate debugging information.
            /// </summary>
            public static bool GenerateDebugInfo
            {
                get => generateDebugInfo;
                set
                {
                    TraceConfigChanges("GenerateDebugInfo", generateDebugInfo, value);
                    generateDebugInfo = value;
                }
            }

            [Flags]
            public enum DebugFlags
            {
                None = 0,
                Batches = 1,
                Operators = 2,
            }

            [Flags]
            public enum MultiStringFlags
            {
                None = 0,
                Wrappers = 1,
                VectorOperations = 2,
            }

            /// <summary>
            /// When true and in Debug builds, code gen operators will break into
            /// the debugger the first time the code gen'ed class gets loaded.
            /// This allows developers to put breakpoints into interesting places
            /// in the code.
            /// </summary>
            public static DebugFlags BreakIntoCodeGen
            {
                get => breakIntoCodeGen;
                set
                {
                    TraceConfigChanges("BreakIntoCodeGen", breakIntoCodeGen, value);
                    breakIntoCodeGen = value;
                }
            }

            /// <summary>
            /// FOR TEST USE ONLY!
            /// When ForceRowBasedExecution is true, this flag is ignored.
            /// When it is false, then the code generation will not fall back
            /// to row-based execution, but instead will throw an (uncaught)
            /// exception. Useful to make sure that tests that are intended to
            /// work in columnar actually do.
            /// </summary>
            internal static bool DontFallBackToRowBasedExecution
            {
                get => dontFallBackToRowBasedExecution;
                set
                {
                    TraceConfigChanges("DontFallBackToRowBasedExecution", dontFallBackToRowBasedExecution, value);
                    dontFallBackToRowBasedExecution = value;
                }
            }

            /// <summary>
            /// FOR TEST USE ONLY!
            /// When ForceRowBasedExecution is true, this flag is ignored.
            /// When it is false, then the code generation will not use
            /// any compiled lambdas, but will insist on inlining all of
            /// them, e.g. in aggregates. Otherwise it will throw an
            /// (uncaught) exception.
            /// Useful to make sure that tests that are intended to
            /// work in columnar actually do.
            /// </summary>
            internal static bool SuperStrictColumnar
            {
                get => superStrictColumnar;
                set
                {
                    TraceConfigChanges("SuperStrictColumnar", superStrictColumnar, value);
                    superStrictColumnar = value;
                }
            }

            /// <summary>
            /// When CodeGenAfa is true, then the code generation will be used
            /// for the pattern matching API. Default: true
            /// </summary>
            internal static bool CodeGenAfa
            {
                get => codeGenAfa;
                set
                {
                    TraceConfigChanges("CodeGenAfa", codeGenAfa, value);
                    codeGenAfa = value;
                }
            }

        }

        [Conditional("DEBUG")]
        private static void TraceConfigChanges<T>(string name, T from, T to)
        {
            // Change to false to disable tracing Config values tracing.
            if (true)
            {
                if (typeof(IInternalScheduler).GetTypeInfo().IsAssignableFrom(typeof(T)))
                {
                    var fromSch = (IInternalScheduler)from;
                    var toSch = (IInternalScheduler)to;
                    Debug.WriteLine("### Config: '{0}' {1} -> {2}", name,
                        SchedToStr(fromSch), SchedToStr(toSch));
                }
                else
                {
                    Debug.WriteLine("### Config: '{0}' {1} -> {2}", name, from, to);
                }
            }
        }

        private static string SchedToStr(IInternalScheduler sch)
            => sch == null
                ? "NULL"
                : string.Format(
                    System.Globalization.CultureInfo.InvariantCulture,
                    "{0}(Map:{1}, Reduce:{2})", sch.GetType().Name, sch.MapArity, sch.ReduceArity);

        /// <summary>
        /// Provides a string representation of the configuration settings.
        /// </summary>
        /// <returns>A string representation of the configuration settings.</returns>
        public static string Describe()
            => new
                {
                    MapArity,
                    ReduceArity,
                    ForceRowBasedExecution,
                    DeterministicWithinTimestamp,
                    ClearColumnsOnReturn,
                    DisableMemoryPooling,
                    DataBatchSize,
                    UseMultiString,
                    IngressSortingTechnique,
                    MultiStringTransforms,
                    Scheduler = SchedToStr(StreamScheduler.scheduler),
                    GeneratedCodePath,
                    CodegenOptions.GenerateDebugInfo,
                    CodegenOptions.BreakIntoCodeGen,
                    CodegenOptions.DontFallBackToRowBasedExecution,
                    CodegenOptions.SuperStrictColumnar,
                    CodegenOptions.CodeGenAfa,
                }.ToString();
    }

    // ConfigModifier allows to modify multiple Config values at once, guaranteeing that only
    // one computation with modified Config values is active at any given time in an AppDomain.
    //
    // Usage:
    //
    // using(new ConfigModifier().ForceRowBasedExecution(true).DataBatchSize(100).Modify())
    // {
    //     // your Trill code here...
    // }
    //
    // or alternatively split var d = ...Modify() and d.Dipose() into test setup and cleanup
    // correspondingly. Be cautious though to ensure Dispose() is always called,
    // as it releases the lock on changing Config via ConfigModifier.
    //
    // Note, that using ConfigModifier would prevent tests that otherwise may run in parallel
    // in VSTest, which is a good thing, since Config is static and those tests may clash otherwise.
    internal sealed class ConfigModifier
    {
        // lockable gate allowing only one ConfigModifier active at a time
        private static readonly object gate = new object();

        // collection of Config modifications
        private readonly List<IGatedModification> modifications = new List<IGatedModification>();

        public ConfigModifier GeneratedCodePath(string value)
        {
            this.modifications.Add(GatedModification<string>.Create(
                value,
                v => { var old = Config.GeneratedCodePath; Config.GeneratedCodePath = v; return old; }));
            return this;
        }

        public ConfigModifier MapArity(int value)
        {
            this.modifications.Add(GatedModification<int>.Create(
                value,
                v => { var old = Config.MapArity; Config.MapArity = v; return old; }));
            return this;
        }

        public ConfigModifier ReduceArity(int value)
        {
            this.modifications.Add(GatedModification<int>.Create(
                value,
                v => { var old = Config.ReduceArity; Config.ReduceArity = v; return old; }));
            return this;
        }

        public ConfigModifier ForceRowBasedExecution(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.ForceRowBasedExecution; Config.ForceRowBasedExecution = v; return old; }));
            return this;
        }

        public ConfigModifier AllowFloatingReorderPolicy(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.AllowFloatingReorderPolicy; Config.AllowFloatingReorderPolicy = v; return old; }));
            return this;
        }

        public ConfigModifier DeterministicWithinTimestamp(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.DeterministicWithinTimestamp; Config.DeterministicWithinTimestamp = v; return old; }));
            return this;
        }

        public ConfigModifier ClearColumnsOnReturn(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.ClearColumnsOnReturn; Config.ClearColumnsOnReturn = v; return old; }));
            return this;
        }

        public ConfigModifier DisableMemoryPooling(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.DisableMemoryPooling; Config.DisableMemoryPooling = v; return old; }));
            return this;
        }

        public ConfigModifier DataBatchSize(int value)
        {
            this.modifications.Add(GatedModification<int>.Create(
                value,
                v => { var old = Config.DataBatchSize; Config.DataBatchSize = v; return old; }));
            return this;
        }

        public ConfigModifier UseMultiString(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.UseMultiString; Config.UseMultiString = v; return old; }));
            return this;
        }

        public ConfigModifier IngressSortingTechnique(SortingTechnique value)
        {
            this.modifications.Add(GatedModification<SortingTechnique>.Create(
                value,
                v => { var old = Config.IngressSortingTechnique; Config.IngressSortingTechnique = v; return old; }));
            return this;
        }

        public ConfigModifier MultiStringTransforms(Config.CodegenOptions.MultiStringFlags value)
        {
            this.modifications.Add(GatedModification<Config.CodegenOptions.MultiStringFlags>.Create(
                value,
                v => { var old = Config.MultiStringTransforms; Config.MultiStringTransforms = v; return old; }));
            return this;
        }

        public ConfigModifier DefaultScheduler(StreamScheduler value)
        {
            this.modifications.Add(GatedModification<StreamScheduler>.Create(
                value,
                v => { var old = Config.StreamScheduler; Config.StreamScheduler = v; return old; }));
            return this;
        }

        public ConfigModifier GenerateDebugInfo(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.CodegenOptions.GenerateDebugInfo; Config.CodegenOptions.GenerateDebugInfo = v; return old; }));
            return this;
        }

        public ConfigModifier BreakIntoCodeGen(Config.CodegenOptions.DebugFlags value)
        {
            this.modifications.Add(GatedModification<Config.CodegenOptions.DebugFlags>.Create(
                value,
                v => { var old = Config.CodegenOptions.BreakIntoCodeGen; Config.CodegenOptions.BreakIntoCodeGen = v; return old; }));
            return this;
        }

        public ConfigModifier DontFallBackToRowBasedExecution(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.CodegenOptions.DontFallBackToRowBasedExecution; Config.CodegenOptions.DontFallBackToRowBasedExecution = v; return old; }));
            return this;
        }

        public ConfigModifier SuperStrictColumnar(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.CodegenOptions.SuperStrictColumnar; Config.CodegenOptions.SuperStrictColumnar = v; return old; }));
            return this;
        }

        public ConfigModifier CodeGenAfa(bool value)
        {
            this.modifications.Add(GatedModification<bool>.Create(
                value,
                v => { var old = Config.CodegenOptions.CodeGenAfa; Config.CodegenOptions.CodeGenAfa = v; return old; }));
            return this;
        }

        public IDisposable Modify()
        {
            Monitor.Enter(gate);
            foreach (var m in this.modifications)
                m.Modify();

            return new ConfigModifierDisposable(() =>
            {
                foreach (var m in this.modifications)
                    m.Modify();
                Monitor.Exit(gate);
            });
        }

        // This is just System.Reactive.Disposables.AnnonymousDisposable.
        // Too small to warrant adding dependency on Rx for this.
        private sealed class ConfigModifierDisposable : IDisposable
        {
            private readonly Action dispose;

            public ConfigModifierDisposable(Action dispose) => this.dispose = dispose;

            public void Dispose() => this.dispose();
        }

        private interface IGatedModification
        {
            void Modify();
        }

        private sealed class GatedModification<T> : IGatedModification
        {
            private T val;
            private Func<T, T> modifier;

            public static GatedModification<T> Create(T newValue, Func<T, T> modifier)
                => new GatedModification<T>
                    {
                        val = newValue,
                        modifier = modifier
                    };

            public void Modify() => this.val = this.modifier(this.val);
        }
    }
}
