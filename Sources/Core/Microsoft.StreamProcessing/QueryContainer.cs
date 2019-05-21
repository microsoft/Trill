// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.StreamProcessing.Serializer;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A container object that represents a stream query.
    /// </summary>
    public sealed class QueryContainer
    {
        private readonly object sentinel = new object();

        /// <summary>
        /// ISurrogate to be used in serialization in checkpoints and serialized StreamMessage
        /// for payload types which can not be serialized otherwise.
        /// </summary>
        public ISurrogate Surrogate { get; }

        private readonly HashSet<string> ingressSites = new HashSet<string>();
        private readonly HashSet<string> egressSites = new HashSet<string>();

        private readonly Dictionary<string, IIngressStreamObserver> ingressPipes = new Dictionary<string, IIngressStreamObserver>();
        private readonly Dictionary<string, IEgressStreamObserver> egressPipes = new Dictionary<string, IEgressStreamObserver>();

        private readonly ConcurrentDictionary<Tuple<string, Type, Type>, Type> sortedDictionaryTypes = new ConcurrentDictionary<Tuple<string, Type, Type>, Type>();
        private readonly ConcurrentDictionary<Tuple<string, Type, Type>, Type> fastDictionaryTypes = new ConcurrentDictionary<Tuple<string, Type, Type>, Type>();
        private readonly ConcurrentDictionary<Tuple<string, Type, Type>, Type> fastDictionary2Types = new ConcurrentDictionary<Tuple<string, Type, Type>, Type>();
        private readonly ConcurrentDictionary<Tuple<string, Type, Type>, Type> fastDictionary3Types = new ConcurrentDictionary<Tuple<string, Type, Type>, Type>();

        private readonly ConcurrentDictionary<Type, object> serializers = new ConcurrentDictionary<Type, object>();

        /// <summary>
        /// Creates a new instance of a query container for use in checkpointable queries.
        /// </summary>
        public QueryContainer() : this(null) { }

        /// <summary>
        /// Creates a new instance of a query container for use in checkpointable queries.
        /// </summary>
        /// <param name="surrogate">An object that offers serialization surrogacy.</param>
        public QueryContainer(ISurrogate surrogate) => this.Surrogate = surrogate;

        internal void RegisterIngressSite(string identifier)
        {
            if (!this.ingressSites.Add(identifier)) throw new InvalidOperationException();
        }

        internal void RegisterIngressPipe(string identifier, IIngressStreamObserver pipe)
        {
            if (!this.ingressSites.Contains(identifier)) throw new InvalidOperationException();
            if (this.ingressPipes.ContainsKey(identifier)) throw new InvalidOperationException();
            this.ingressPipes.Add(identifier, pipe);
        }

        internal void RegisterEgressSite(string identifier)
        {
            if (!this.egressSites.Add(identifier)) throw new InvalidOperationException();
        }

        internal void RegisterEgressPipe(string identifier, IEgressStreamObserver pipe)
        {
            if (!this.egressSites.Contains(identifier)) throw new InvalidOperationException();
            if (this.egressPipes.ContainsKey(identifier)) throw new InvalidOperationException();
            this.egressPipes.Add(identifier, pipe);
        }

        internal void RegisterSortedDictionaryType(Tuple<string, Type, Type> key, Type type)
            => this.sortedDictionaryTypes[key] = type;

        internal void RegisterFastDictionaryType(Tuple<string, Type, Type> key, Type type)
            => this.fastDictionaryTypes[key] = type;

        internal void RegisterFastDictionary2Type(Tuple<string, Type, Type> key, Type type)
            => this.fastDictionary2Types[key] = type;

        internal void RegisterFastDictionary3Type(Tuple<string, Type, Type> key, Type type)
            => this.fastDictionary3Types[key] = type;

        internal bool TryGetSortedDictionaryType(Tuple<string, Type, Type> key, out Type type)
            => this.sortedDictionaryTypes.TryGetValue(key, out type);

        internal bool TryGetFastDictionaryType(Tuple<string, Type, Type> key, out Type type)
            => this.fastDictionaryTypes.TryGetValue(key, out type);

        internal bool TryGetFastDictionary2Type(Tuple<string, Type, Type> key, out Type type)
            => this.fastDictionary2Types.TryGetValue(key, out type);

        internal bool TryGetFastDictionary3Type(Tuple<string, Type, Type> key, out Type type)
            => this.fastDictionary3Types.TryGetValue(key, out type);

        internal object GetOrCreateSerializer(Type type)
        {
            if (this.serializers.TryGetValue(type, out object serializer)) return serializer;
            var serializerStatic = typeof(StreamableSerializer);
            var method = serializerStatic.GetTypeInfo().GetMethod("Create", new Type[] { typeof(SerializerSettings) }).MakeGenericMethod(type);
            var settings = new SerializerSettings()
            {
                KnownTypes = this.CollectedGeneratedTypes,
                Surrogate = this.Surrogate,
            };
            serializer = method.Invoke(/* static */ null, new object[] { settings });
            this.serializers[type] = serializer;
            return serializer;
        }

        internal IEnumerable<Type> CollectedGeneratedTypes
            => this.sortedDictionaryTypes.Values.Concat(this.fastDictionaryTypes.Values).Concat(this.fastDictionary2Types.Values).Concat(this.fastDictionary3Types.Values).Concat(
               StreamMessageManager.GeneratedTypes());

        /// <summary>
        /// Start a query, with or without a previously checkpointed state.
        /// </summary>
        /// <param name="inputStream">The stream from which query state should be retrieved.</param>
        /// <returns>A Process object that represents an active, running query that can be checkpointed.</returns>
        public Process Restore(Stream inputStream = null)
        {
            lock (this.sentinel)
            {
                // Restoration should not happen until after all streams have been both registered and subscribed
                if (this.ingressSites.Count != this.ingressPipes.Count) throw new StreamProcessingException("Not all output data sources have been subscribed to.");
                if (this.egressSites.Count != this.egressPipes.Count) throw new StreamProcessingException("Not all output data sources have been subscribed to.");

                var process = new Process(this.ingressPipes.Clone(), this.egressPipes.Clone());

                process.Restore(inputStream);

                this.ingressPipes.Clear();
                this.egressPipes.Clear();

                return process;
            }
        }
    }

    /// <summary>
    /// A class representing a running query that can be checkpointed.
    /// </summary>
    public sealed class Process
    {
        private const int CheckpointVersionMajor = 2;
        private const int CheckpointVersionMinor = 0;
        private const int CheckpointVersionRevision = 0;

        private readonly object sentinel = new object();
        private readonly Dictionary<string, IIngressStreamObserver> IngressPipes;
        private Dictionary<string, PlanNode> queryPlans;

        internal Process(Dictionary<string, IIngressStreamObserver> ingress, Dictionary<string, IEgressStreamObserver> egress)
        {
            this.IngressPipes = ingress;
            foreach (var e in egress)
            {
                e.Value.AttachProcess(e.Key, this);
            }
        }

        /// <summary>
        /// Quiesce the currently running query and checkpoint its state to the given stream.
        /// </summary>
        /// <param name="outputStream">The stream to which the checkpoint is recorded.</param>
        public void Checkpoint(Stream outputStream)
        {
            Invariant.IsNotNull(outputStream, nameof(outputStream));
            lock (this.sentinel)
            {
                outputStream.Write(BitConverter.GetBytes(CheckpointVersionMajor), 0, sizeof(int));
                outputStream.Write(BitConverter.GetBytes(CheckpointVersionMinor), 0, sizeof(int));
                outputStream.Write(BitConverter.GetBytes(CheckpointVersionRevision), 0, sizeof(int));

                try
                {
                    foreach (var pipe in this.IngressPipes.Values)
                    {
                        pipe.Checkpoint(outputStream);
                    }
                }
                catch (Exception)
                {
                    foreach (var pipe in this.IngressPipes.Values)
                    {
                        pipe.Reset();
                    }
                    throw;
                }
            }
        }

        internal void Restore(Stream inputStream)
        {
            lock (this.sentinel)
            {
                if (inputStream != null)
                {
                    byte[] buffer = new byte[sizeof(int)];
                    inputStream.Read(buffer, 0, sizeof(int));
                    int major = BitConverter.ToInt32(buffer, 0);
                    inputStream.Read(buffer, 0, sizeof(int));
                    int minor = BitConverter.ToInt32(buffer, 0);
                    inputStream.Read(buffer, 0, sizeof(int));
                    int revision = BitConverter.ToInt32(buffer, 0);

                    if (major != CheckpointVersionMajor || minor != CheckpointVersionMinor || revision != CheckpointVersionRevision)
                    {
                        throw new StreamProcessingException(
                            string.Format(
                                CultureInfo.InvariantCulture,
                                "Version mismatch between the stream state and the engine.  Expected: {0}.{1}.{2}, Found: {3}.{4}.{5}",
                                CheckpointVersionMajor,
                                CheckpointVersionMinor,
                                CheckpointVersionRevision,
                                major,
                                minor,
                                revision));
                    }
                }

                try
                {
                    foreach (var pipe in this.IngressPipes.Values)
                    {
                        pipe.Restore(inputStream);
                    }
                }
                catch (Exception)
                {
                    foreach (var pipe in this.IngressPipes.Values)
                    {
                        pipe.Reset();
                    }
                    throw;
                }
            }
        }

        /// <summary>
        /// Flushes batched output events
        /// </summary>
        public void Flush()
        {
            lock (this.sentinel)
            {
                try
                {
                    foreach (var pipe in this.IngressPipes.Values)
                    {
                        pipe.OnFlush();
                    }
                }
                catch (Exception)
                {
                    foreach (var pipe in this.IngressPipes.Values)
                    {
                        pipe.Reset();
                    }
                    throw;
                }
            }
        }

        /// <summary>
        /// Returns the set of ingress sites that may be blocking result creation due to
        /// lack of input data.
        /// </summary>
        public Dictionary<string, IngressPlanNode> PotentiallyQuietIngressSites
        {
            get
            {
                var nodes = new Dictionary<string, IngressPlanNode>();
                foreach (var i in this.QueryPlan)
                {
                    i.Value.CollectDormantIngressSites(nodes);
                }
                return nodes;
            }
        }

        /// <summary>
        /// Returns the internal query plan of the actively running stream query rooted at the given point of egress.
        /// </summary>
        public PlanNode GetQueryPlanAtEgress(string identifier) => this.QueryPlan[identifier];

        /// <summary>
        /// Returns the internal query plan of the actively running stream query.
        /// </summary>
        public Dictionary<string, PlanNode> QueryPlan
        {
            get
            {
                if (this.queryPlans == null)
                {
                    this.queryPlans = new Dictionary<string, PlanNode>();
                    foreach (var i in this.IngressPipes)
                    {
                        i.Value.ProduceQueryPlan(null);
                    }
                }

                return this.queryPlans;
            }
        }

        internal void RegisterQueryPlan(string identifier, PlanNode node) => this.queryPlans.Add(identifier, node);
    }
}
