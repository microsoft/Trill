// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Globalization;

namespace Microsoft.StreamProcessing
{
    internal abstract class CommonPipeTemplate : CommonBaseTemplate
    {
        protected readonly string staticCtor;
        protected readonly string className;

        protected CommonPipeTemplate(string className)
        {
            this.className = className;
            this.staticCtor = StaticCtor(className);
        }

        private static string StaticCtor(string className)
        {
            var includeDebugInfo = Config.CodegenOptions.GenerateDebugInfo
                && ((Config.CodegenOptions.BreakIntoCodeGen & Config.CodegenOptions.DebugFlags.Operators) != 0);
#if DEBUG
            includeDebugInfo = (Config.CodegenOptions.BreakIntoCodeGen & Config.CodegenOptions.DebugFlags.Operators) != 0;
#endif
            return includeDebugInfo
                ? string.Format(
                    CultureInfo.InvariantCulture,
@"
    static {0}() {{
        if (System.Diagnostics.Debugger.IsAttached)
            System.Diagnostics.Debugger.Break();
        else
            System.Diagnostics.Debugger.Launch();
    }}", className)
                : string.Empty;
        }
    }
}