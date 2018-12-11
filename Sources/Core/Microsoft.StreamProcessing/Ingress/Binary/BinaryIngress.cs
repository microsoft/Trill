// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using Microsoft.StreamProcessing.Serializer;

namespace Microsoft.StreamProcessing
{
    public static partial class Streamable
    {
        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="binaryStream"></param>
        /// <param name="scheduler"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns></returns>
        public static IIngressStreamable<Empty, TPayload> ToStreamable<TPayload>(
            this Stream binaryStream,
            IIngressScheduler scheduler = null,
            bool readPropertiesFromStream = false,
            StreamProperties<Empty, TPayload> inputProperties = null)
        {
            return new BinaryIngressStreamable<Empty, TPayload>(
                binaryStream,
                0,
                scheduler,
                inputProperties,
                readPropertiesFromStream,
                null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="fileName"></param>
        /// <param name="numMessages"></param>
        /// <param name="scheduler"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns></returns>
        public static IIngressStreamable<Empty, TPayload> ToStreamableFromFile<TPayload>(
            this string fileName,
            int numMessages = 0,
            IIngressScheduler scheduler = null,
            bool readPropertiesFromStream = false,
            StreamProperties<Empty, TPayload> inputProperties = null)
        {
            return new BinaryIngressStreamable<Empty, TPayload>(
                new FileStream(fileName, FileMode.Open),
                numMessages,
                scheduler,
                inputProperties,
                readPropertiesFromStream,
                null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="binaryStream"></param>
        /// <param name="scheduler"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns></returns>
        public static IIngressStreamable<TKey, TPayload> ToStreamable<TKey, TPayload>(
            this Stream binaryStream,
            IIngressScheduler scheduler = null,
            bool readPropertiesFromStream = false,
            StreamProperties<TKey, TPayload> inputProperties = null)
        {
            return new BinaryIngressStreamable<TKey, TPayload>(
                binaryStream,
                0,
                scheduler,
                inputProperties,
                readPropertiesFromStream,
                null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="fileName"></param>
        /// <param name="numMessages"></param>
        /// <param name="scheduler"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns></returns>
        public static IIngressStreamable<TKey, TPayload> ToStreamableFromFile<TKey, TPayload>(
            this string fileName,
            int numMessages = 0,
            IIngressScheduler scheduler = null,
            bool readPropertiesFromStream = false,
            StreamProperties<TKey, TPayload> inputProperties = null)
        {
            return new BinaryIngressStreamable<TKey, TPayload>(
                new FileStream(fileName, FileMode.Open),
                numMessages,
                scheduler,
                inputProperties,
                readPropertiesFromStream,
                null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="binaryStream"></param>
        /// <param name="scheduler"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <param name="container"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        public static IIngressStreamable<Empty, TPayload> RegisterBinaryInput<TPayload>(
            this QueryContainer container,
            Stream binaryStream,
            IIngressScheduler scheduler = null,
            bool readPropertiesFromStream = false,
            StreamProperties<Empty, TPayload> inputProperties = null,
            string identifier = null)
        {
            return new BinaryIngressStreamable<Empty, TPayload>(
                binaryStream,
                0,
                scheduler,
                inputProperties,
                readPropertiesFromStream,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="fileName"></param>
        /// <param name="numMessages"></param>
        /// <param name="scheduler"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <param name="container"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        public static IIngressStreamable<Empty, TPayload> RegisterBinaryInputFromFile<TPayload>(
            this QueryContainer container,
            string fileName,
            int numMessages = 0,
            IIngressScheduler scheduler = null,
            bool readPropertiesFromStream = false,
            StreamProperties<Empty, TPayload> inputProperties = null,
            string identifier = null)
        {
            return new BinaryIngressStreamable<Empty, TPayload>(
                new FileStream(fileName, FileMode.Open),
                numMessages,
                scheduler,
                inputProperties,
                readPropertiesFromStream,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="binaryStream"></param>
        /// <param name="scheduler"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <param name="container"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        public static IIngressStreamable<TKey, TPayload> RegisterBinaryInput<TKey, TPayload>(
            this QueryContainer container,
            Stream binaryStream,
            IIngressScheduler scheduler = null,
            bool readPropertiesFromStream = false,
            StreamProperties<TKey, TPayload> inputProperties = null,
            string identifier = null)
        {
            return new BinaryIngressStreamable<TKey, TPayload>(
                binaryStream,
                0,
                scheduler,
                inputProperties,
                readPropertiesFromStream,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="fileName"></param>
        /// <param name="numMessages"></param>
        /// <param name="scheduler"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <param name="container"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        public static IIngressStreamable<TKey, TPayload> RegisterBinaryInputFromFile<TKey, TPayload>(
            this QueryContainer container,
            string fileName,
            int numMessages = 0,
            IIngressScheduler scheduler = null,
            bool readPropertiesFromStream = false,
            StreamProperties<TKey, TPayload> inputProperties = null,
            string identifier = null)
        {
            return new BinaryIngressStreamable<TKey, TPayload>(
                new FileStream(fileName, FileMode.Open),
                numMessages,
                scheduler,
                inputProperties,
                readPropertiesFromStream,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="binaryStream"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns></returns>
        public static IPassiveIngressStreamable<Empty, TPayload> ToStreamablePassive<TPayload>(
            this Stream binaryStream,
            bool readPropertiesFromStream = false,
            StreamProperties<Empty, TPayload> inputProperties = null)
        {
            return new BinaryIngressStreamablePassive<Empty, TPayload>(
                binaryStream,
                inputProperties,
                readPropertiesFromStream,
                null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="fileName"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns></returns>
        public static IPassiveIngressStreamable<Empty, TPayload> ToStreamablePassiveFromFile<TPayload>(
            this string fileName,
            bool readPropertiesFromStream = false,
            StreamProperties<Empty, TPayload> inputProperties = null)
        {
            return new BinaryIngressStreamablePassive<Empty, TPayload>(
                new FileStream(fileName, FileMode.Open),
                inputProperties,
                readPropertiesFromStream,
                null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="binaryStream"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns></returns>
        public static IPassiveIngressStreamable<TKey, TPayload> ToStreamablePassive<TKey, TPayload>(
            this Stream binaryStream,
            bool readPropertiesFromStream = false,
            StreamProperties<TKey, TPayload> inputProperties = null)
        {
            return new BinaryIngressStreamablePassive<TKey, TPayload>(
                binaryStream,
                inputProperties,
                readPropertiesFromStream,
                null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="fileName"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns></returns>
        public static IPassiveIngressStreamable<TKey, TPayload> ToStreamablePassiveFromFile<TKey, TPayload>(
            this string fileName,
            bool readPropertiesFromStream = false,
            StreamProperties<TKey, TPayload> inputProperties = null)
        {
            return new BinaryIngressStreamablePassive<TKey, TPayload>(
                new FileStream(fileName, FileMode.Open),
                inputProperties,
                readPropertiesFromStream,
                null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="binaryStream"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <param name="container"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        public static IPassiveIngressStreamable<Empty, TPayload> RegisterBinaryInputPassive<TPayload>(
            this QueryContainer container,
            Stream binaryStream,
            bool readPropertiesFromStream = false,
            StreamProperties<Empty, TPayload> inputProperties = null,
            string identifier = null)
        {
            return new BinaryIngressStreamablePassive<Empty, TPayload>(
                binaryStream,
                inputProperties,
                readPropertiesFromStream,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="fileName"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <param name="container"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        public static IPassiveIngressStreamable<Empty, TPayload> RegisterBinaryInputPassiveFromFile<TPayload>(
            this QueryContainer container,
            string fileName,
            bool readPropertiesFromStream = false,
            StreamProperties<Empty, TPayload> inputProperties = null,
            string identifier = null)
        {
            return new BinaryIngressStreamablePassive<Empty, TPayload>(
                new FileStream(fileName, FileMode.Open),
                inputProperties,
                readPropertiesFromStream,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="binaryStream"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <param name="container"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        public static IPassiveIngressStreamable<TKey, TPayload> RegisterBinaryInputPassive<TKey, TPayload>(
            this QueryContainer container,
            Stream binaryStream,
            bool readPropertiesFromStream = false,
            StreamProperties<TKey, TPayload> inputProperties = null,
            string identifier = null)
        {
            return new BinaryIngressStreamablePassive<TKey, TPayload>(
                binaryStream,
                inputProperties,
                readPropertiesFromStream,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Deserialize from binary stream to streamable
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="fileName"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <param name="container"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        public static IPassiveIngressStreamable<TKey, TPayload> RegisterBinaryInputPassiveFromFile<TKey, TPayload>(
            this QueryContainer container,
            string fileName,
            bool readPropertiesFromStream = false,
            StreamProperties<TKey, TPayload> inputProperties = null,
            string identifier = null)
        {
            return new BinaryIngressStreamablePassive<TKey, TPayload>(
                new FileStream(fileName, FileMode.Open),
                inputProperties,
                readPropertiesFromStream,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

    }
}
