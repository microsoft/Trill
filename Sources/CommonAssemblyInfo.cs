/*
The following assembly information is common to all product assemblies.
If you get compiler errors CS0579, "Duplicate '<attributename>' attribute", check your
Properties\AssemblyInfo.cs file and remove any lines duplicating the ones below.
[assembly: AssemblyCompany("Microsoft Corporation")]
[assembly: AssemblyCopyright("Copyright (c) Microsoft Corporation. All rights reserved.")]
[assembly: AssemblyTrademark("Microsoft and Windows are either registered trademarks or trademarks of Microsoft Corporation in the U.S. and/or other countries.")]
[assembly: AssemblyProduct("Microsoft.StreamProcessing")]
[assembly: AssemblyCulture("")]
#if (DEBUG || _DEBUG)
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("")]
#endif

Version information for an assembly consists of the following four values:

     Major Version
     Minor Version
     Build Number
     Revision

You can specify all the values or you can default the Build and Revision Numbers
by using the '*' as shown below:
[assembly: AssemblyVersion("1.0.*")]
[assembly: AssemblyVersion("1.0.60426.1")]
[assembly: AssemblyFileVersion("1.0.0.0")]
*/

/// <summary>
/// Sets public key string for friend assemblies.
/// </summary>
internal static class AssemblyRef
{
    internal const string ProductPublicKey = ",PublicKey=" +
    "0024000004800000940000000602000000240000525341310004000001000100850b8b20bfa473" +
    "b6075084208ca17296540c8f8a9a7ac16c6f8d658235e5028266ea864a9750b884dcd388ce4736" +
    "15e772af1434a379113feff048ed1f6774f0eb05efde1ac9baf1eb4428a6e28dc936f0b9ba98e0" +
    "e23f96c8e962231b3eb7214ffb085d5f147d7cd66ed0e04c9b63fcb4b4f7ce5ebcc203cf2d9c19" +
    "dbe4b7c1";
}
