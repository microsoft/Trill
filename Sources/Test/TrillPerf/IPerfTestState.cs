// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace PerformanceTesting
{
    public interface IPerfTestState
    {
        string Name { get; set; }

        string Action { get; set; }

        void AddResult(long inputRows, long outputRows, TimeSpan timing);

        void AddMessage(string message);
    }
}
