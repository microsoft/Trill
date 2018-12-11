// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Represents either a positive or negative tuple in an ordered stream of atemporal events.
    /// </summary>
    internal static class ChangeListEvent
    {
        /// <summary>
        /// Creates an event from a payload, corresponding to an insertion into a dataset
        /// </summary>
        /// <param name="payload">The payload value to be inserted</param>
        /// <returns>Returns a change list insertion event</returns>
        public static ChangeListEvent<TPayload> CreateInsertion<TPayload>(TPayload payload)
        {
            return new ChangeListEvent<TPayload>
            {
                EventKind = ChangeListEventKind.Insert,
                Payload = payload
            };
        }

        /// <summary>
        /// Creates an event from a payload, corresponding to an deletion from a dataset
        /// </summary>
        /// <param name="payload">The payload value to be deleted</param>
        /// <returns>Returns a change list deletion event</returns>
        public static ChangeListEvent<TPayload> CreateDeletion<TPayload>(TPayload payload)
        {
            return new ChangeListEvent<TPayload>
            {
                EventKind = ChangeListEventKind.Delete,
                Payload = payload
            };
        }
    }
}
