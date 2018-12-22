using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;


[assembly: ComVisible(false)]


#pragma warning disable 0436

[assembly: InternalsVisibleTo("Microsoft.StreamProcessing.Provider" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.StreamProcessing.NRT" + AssemblyRef.ProductPublicKey)]

// Playground
[assembly: InternalsVisibleTo("ProgressiveSarthak" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("SchedulerTest" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("SerializationTest" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("SignalTest" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("ToyExamples" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("TpchData" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("UserQueryData2" + AssemblyRef.ProductPublicKey)]

// Test
[assembly: InternalsVisibleTo("SimpleTesting" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("TrillPerf" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("TrillTest" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("TrillTestHarness" + AssemblyRef.ProductPublicKey)]

#pragma warning restore 0436
