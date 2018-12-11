// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.StreamProcessing.Serializer
{
    /// <summary>
    ///     Specifies serializer settings.
    /// </summary>
    internal sealed class SerializerSettings : IEquatable<SerializerSettings>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="SerializerSettings" /> class.
        /// </summary>
        public SerializerSettings()
        {
            this.MaxItemsInSchemaTree = 1024;
            this.KnownTypes = new List<Type>();
        }

        /// <summary>
        /// Gets or sets a serialization surrogate.
        /// </summary>
        public ISurrogate Surrogate { get; set; }

        /// <summary>
        ///     Gets or sets the maximum number of items in the schema tree.
        /// </summary>
        /// <value>
        ///     The maximum number of items in the schema tree.
        /// </value>
        public int MaxItemsInSchemaTree { get; set; }

        /// <summary>
        ///     Gets or sets the known types.
        /// </summary>
        public IEnumerable<Type> KnownTypes { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to use a cache of precompiled serializers.
        /// </summary>
        /// <value>
        ///   <c>True</c> if to use the cache; otherwise, <c>false</c>.
        /// </value>
        public bool UseCache { get; set; }

        /// <summary>
        ///     Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        ///     True if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
        /// </returns>
        public bool Equals(SerializerSettings other)
        {
            if (other == null) return false;

            return this.MaxItemsInSchemaTree == other.MaxItemsInSchemaTree
                && this.Surrogate == other.Surrogate
                && this.UseCache == other.UseCache
                && this.KnownTypes.SequenceEqual(other.KnownTypes);
        }

        /// <summary>
        ///     Determines whether the specified <see cref="object" /> is equal to this instance.
        /// </summary>
        /// <param name="obj">
        ///     The <see cref="object" /> to compare with this instance.
        /// </param>
        /// <returns>
        ///     <c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj) => Equals(obj as SerializerSettings);

        /// <summary>
        ///     Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        ///     A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                var hashcode = 83;
                hashcode = (hashcode * 89) + this.MaxItemsInSchemaTree.GetHashCode();
                hashcode = (hashcode * 89) + (this.Surrogate != null ? this.Surrogate.GetHashCode() : 0);
                hashcode = (hashcode * 89) + this.UseCache.GetHashCode();
                if (this.KnownTypes != null)
                {
                    hashcode = (hashcode * 89) + this.KnownTypes.Count();
                    foreach (var knownType in this.KnownTypes)
                    {
                        hashcode = (hashcode * 89) + (knownType != null ? knownType.GetHashCode() : 0);
                    }
                }
                return hashcode;
            }
        }
    }
}
