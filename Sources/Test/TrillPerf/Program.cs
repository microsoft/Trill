// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace PerformanceTesting
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(
                "Trill Performance Tester\n" +
                "---");
            PerfTestManager.RunAllTests();
            Console.ReadKey();
        }
    }
}
