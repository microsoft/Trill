// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: ComVisible(false)]

#pragma warning disable 0436

// Test
[assembly: InternalsVisibleTo("SimpleTesting" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("ComponentTesting" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("PerformanceTesting" + AssemblyRef.ProductPublicKey)]

#pragma warning restore 0436
