// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static class NativeMethods
    {
        [DllImport("kernel32.dll")]
        internal static extern IntPtr GetCurrentThread();
        [DllImport("kernel32")]
        internal static extern uint GetCurrentThreadId();
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern uint GetCurrentProcessorNumber();
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern uint GetActiveProcessorCount(uint count);
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern ushort GetActiveProcessorGroupCount();

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern int SetThreadGroupAffinity(IntPtr hThread, ref GROUP_AFFINITY GroupAffinity, ref GROUP_AFFINITY PreviousGroupAffinity);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern int GetThreadGroupAffinity(IntPtr hThread, ref GROUP_AFFINITY PreviousGroupAffinity);

        internal static uint ALL_PROCESSOR_GROUPS = 0xffff;

        [StructLayout(LayoutKind.Sequential)]
        internal struct GROUP_AFFINITY
        {
            public ulong Mask;
            public uint Group;
            public uint Reserved1;
            public uint Reserved2;
            public uint Reserved3;
        }

        internal static void AffinitizeThreadNuma(uint threadIdx)
        {
            uint nrOfProcessors = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
            ushort nrOfProcessorGroups = GetActiveProcessorGroupCount();
            uint nrOfProcsPerGroup = nrOfProcessors / nrOfProcessorGroups;

            GROUP_AFFINITY groupAffinityThread = default;
            GROUP_AFFINITY oldAffinityThread = default;

            IntPtr thread = GetCurrentThread();
            GetThreadGroupAffinity(thread, ref groupAffinityThread);

            groupAffinityThread.Mask = (ulong)1L << ((int)(threadIdx / nrOfProcessorGroups) % (int)nrOfProcsPerGroup);
            groupAffinityThread.Group = threadIdx % nrOfProcessorGroups;

            if (SetThreadGroupAffinity(thread, ref groupAffinityThread, ref oldAffinityThread) == 0)
            {
                Console.WriteLine("Unable to set group affinity");
            }

        }

        internal static void AffinitizeThread(int processor)
        {
            uint utid = GetCurrentThreadId();
            foreach (ProcessThread pt in System.Diagnostics.Process.GetCurrentProcess().Threads)
            {
                if (utid == pt.Id)
                {
                    long AffinityMask = 1 << processor;
                    pt.ProcessorAffinity = (IntPtr)(AffinityMask); // Set affinity for this
                }
            }
        }
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static class SafeNativeMethods
    {
        private const string lib = "kernel32.dll";
        [DllImport(lib)]
        internal static extern int QueryPerformanceCounter(ref long count);
        [DllImport(lib)]
        internal static extern int QueryPerformanceFrequency(ref long frequency);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static long Frequency()
        {
            long frequency = 0;
            QueryPerformanceFrequency(ref frequency);
            return frequency;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static long Counter()
        {
            long tickCount = 0;
            QueryPerformanceCounter(ref tickCount);
            return tickCount;
        }
    }
}