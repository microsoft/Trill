// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using Microsoft.StreamProcessing.Serializer;

namespace Microsoft.StreamProcessing
{
    internal sealed class BinaryStreamObserver<TKey, TPayload> : IObserver<StreamMessage<TKey, TPayload>>, IDisposable
    {
        private readonly StateSerializer<QueuedMessage<StreamMessage<TKey, TPayload>>> serializer;
        private Stream stream;

        public BinaryStreamObserver(StreamProperties<TKey, TPayload> streamProperties, Stream stream)
        {
            this.serializer = StreamableSerializer.Create<QueuedMessage<StreamMessage<TKey, TPayload>>>(
                new SerializerSettings() { KnownTypes = StreamMessageManager.GeneratedTypes() });
            this.stream = stream;
        }

        public void Dispose()
        {
            this.stream?.Dispose();
            this.stream = null;
        }

        public void OnCompleted()
        {
            var completed = new QueuedMessage<StreamMessage<TKey, TPayload>> { Kind = MessageKind.Completed };
            this.serializer.Serialize(this.stream, completed);
            this.stream.Flush();
        }

        public void OnError(Exception error) => throw error;

        public void OnNext(StreamMessage<TKey, TPayload> message)
        {
            this.serializer.Serialize(this.stream, new QueuedMessage<StreamMessage<TKey, TPayload>> { Kind = MessageKind.DataBatch, Message = message });
            message.Free();
            this.stream.Flush();
        }
    }
}
