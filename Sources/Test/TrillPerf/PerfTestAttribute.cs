// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace PerformanceTesting
{
    public class PerfTestAttribute : Attribute
    {
        public readonly string Name;

        public PerfTestAttribute(string name) => this.Name = name;
    }
}
