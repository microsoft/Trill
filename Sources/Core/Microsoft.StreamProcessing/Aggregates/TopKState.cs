using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.StreamProcessing.Aggregates
{
    /// <summary>
    /// State used by TopK Aggregate
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ITopKState<T>
    {
        /// <summary>
        /// Add a single entry
        /// </summary>
        /// <param name="input"></param>
        /// <param name="timestamp"></param>
        ITopKState<T> Add(T input, long timestamp);

        /// <summary>
        /// Removes the specified entry
        /// </summary>
        /// <param name="input"></param>
        /// <param name="timestamp"></param>
        ITopKState<T> Remove(T input, long timestamp);

        /// <summary>
        /// Removes entries from other
        /// </summary>
        /// <param name="other"></param>
        ITopKState<T> RemoveAll(ITopKState<T> other);

        /// <summary>
        /// Gets the values as sorted set
        /// </summary>
        /// <returns></returns>
        SortedMultiSet<T> GetSortedValues();

        /// <summary>
        /// Returns total number of values in the set
        /// </summary>
        long Count { get; }
    }

    internal class SimpleTopKState<T> : ITopKState<T>
    {
        private SortedMultiSet<T> values;

        public SimpleTopKState(Func<SortedDictionary<T, long>> generator)
        {
            this.values = new SortedMultiSet<T>(generator);
        }

        public long Count => this.values.TotalCount;

        public ITopKState<T> Add(T input, long timestamp)
        {
            this.values.Add(input);
            return this;
        }

        public SortedMultiSet<T> GetSortedValues() => this.values;

        public ITopKState<T> Remove(T input, long timestamp)
        {
            this.values.Remove(input);
            return this;
        }

        public ITopKState<T> RemoveAll(ITopKState<T> other)
        {
            this.values.RemoveAll(other.GetSortedValues());
            return this;
        }
    }

    internal class HoppingTopKState<T> : ITopKState<T>
    {
        public long currentTimestamp;

        public CircularBuffer<ValueTuple<long, SortedMultiSet<T>>> previousValues;
        public SortedMultiSet<T> currentValues;

        public int k;

        public Comparison<T> rankComparer;
        private Func<SortedDictionary<T, long>> generator;
        private ItemAndCount<T> minValue; // The minimum threshold value in TopK

        public HoppingTopKState(int k, Comparison<T> rankComparer, int hoppingWindowSize, Func<SortedDictionary<T, long>> generator)
        {
            this.k = k;
            this.rankComparer = rankComparer;
            this.currentValues = new SortedMultiSet<T>(generator);
            this.previousValues = new CircularBuffer<ValueTuple<long, SortedMultiSet<T>>>(hoppingWindowSize);
            this.generator = generator;
        }

        public ITopKState<T> Add(T input, long timestamp)
        {
            // Verify that input is added in non-decreasing order
            if (timestamp < this.currentTimestamp)
            {
                throw new ArgumentException("Invalid timestamp");
            }

            // First entry in new hop window, just add the value
            if (timestamp > this.currentTimestamp)
            {
                MergeCurrentToPrevious();
                this.currentTimestamp = timestamp;
                this.currentValues.Add(input);
                this.minValue = new ItemAndCount<T>(input, 1);
                return this;
            }

            // these are subsequent entries
            int compare = rankComparer(input, this.minValue.Item);

            if (this.currentValues.TotalCount < this.k) // if we have not reached k yet, add it
            {
                if (compare > 0)
                    this.minValue = new ItemAndCount<T>(input, 1);
                else if (compare == 0)
                    this.minValue.Count++;

                this.currentValues.Add(input);
                return this;
            }
            else if (compare > 0) // We have reached k and new input is smaller than minimum
            {
                return this;
            }
            else if (compare == 0) // We have reached k and new input is equal to the minimum
            {
                this.currentValues.Add(input); // add to get ties
                this.minValue.Count++;
                return this;
            }
            else // The new item is less than minValue, so we need to remove some entries to make place for the new entry
            {
                this.currentValues.Add(input);
                var toRemove = this.currentValues.TotalCount - this.k;
                if (toRemove >= minValue.Count)
                {
                    this.currentValues.RemoveAll(this.minValue.Item);
                    this.minValue = this.currentValues.GetMinItem();
                }
                return this;
            }
        }

        public ITopKState<T> Remove(T input, long timestamp)
        {
            throw new NotImplementedException("Cannot remove single elements from this state");
        }

        public ITopKState<T> RemoveAll(ITopKState<T> other)
        {
            if (other.Count != 0)
            {
                if (other is HoppingTopKState<T> otherTopK)
                {
                    if (otherTopK.currentTimestamp > this.currentTimestamp)
                    {
                        throw new ArgumentException("Cannot remove entries with current or future timestamp");
                    }
                    else if (otherTopK.currentTimestamp == this.currentTimestamp)
                    {
                        if (this.currentValues.TotalCount != otherTopK.currentValues.TotalCount)
                            throw new InvalidOperationException("Invalid removal");

                        this.currentValues.Clear();
                        this.previousValues.Clear();
                    }
                    else
                    {
                        while (this.previousValues.Count > 0)
                        {
                            var first = this.previousValues.PeekFirst();

                            if (first.Item1 > otherTopK.currentTimestamp)
                            {
                                break;
                            }

                            if (first.Item1 == otherTopK.currentTimestamp &&
                                first.Item2.TotalCount != otherTopK.currentValues.TotalCount)
                                throw new InvalidOperationException("Invalid removal");

                            this.previousValues.Dequeue();
                        }
                    }
                }
                else
                {
                    throw new InvalidOperationException("Cannot remove non-HoppingTopKState from HoppingTopKState");
                }
            }
            return this;
        }

        // This function merges the current values to previous and is expensive
        // Currently it is only called by ComputeResult
        public SortedMultiSet<T> GetSortedValues()
        {
            var sortedMultiset = new SortedMultiSet<T>(generator);

            foreach (var dictItem in this.previousValues.Iterate())
            {
                sortedMultiset.AddAll(dictItem.Item2);
            }
            sortedMultiset.AddAll(this.currentValues);

            return sortedMultiset;
        }

        private void MergeCurrentToPrevious()
        {
            if (!this.currentValues.IsEmpty)
            {
                var newEntry = ValueTuple.Create(this.currentTimestamp, this.currentValues);
                this.previousValues.Enqueue(ref newEntry);
                this.currentValues = new SortedMultiSet<T>(generator);
            }
        }

        public long Count => this.currentValues.TotalCount + this.previousValues.Iterate().Sum(e => e.Item2.TotalCount);
    }
}
