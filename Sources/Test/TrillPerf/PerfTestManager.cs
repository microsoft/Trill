// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace PerformanceTesting
{
    public class PerfTestManager
    {
        public delegate void PerfTestDelegate(IPerfTestState state);

        public static void RunAllTests() => RunTests(GetAllTests());

        public static void RunTests(IEnumerable<NamedTest> tests)
        {
            foreach (var test in tests)
            {
                var state = new TestState(test.Name);
                test.Test(state);
                state.DisplaySummary();
            }
        }

        private static IEnumerable<NamedTest> GetAllTests()
        {
            foreach (var assemblyType in Assembly.GetExecutingAssembly().GetExportedTypes())
            {
                var typeMethods =
                    assemblyType.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.DeclaredOnly)
                            .Where(method => !method.IsSpecialName);
                foreach (var method in typeMethods)
                {
                    var attr = (PerfTestAttribute)Attribute.GetCustomAttribute(method, typeof(PerfTestAttribute));

                    if (attr == null)
                    {
                        continue;
                    }

                    var test = (PerfTestDelegate)Delegate.CreateDelegate(typeof(PerfTestDelegate), method, true);

                    yield return new NamedTest(attr.Name, test);
                }
            }
        }

        public struct NamedTest
        {
            public readonly string Name;

            public readonly PerfTestDelegate Test;

            public NamedTest(string name, PerfTestDelegate test)
            {
                this.Name = name;
                this.Test = test;
            }
        }

        private class TestState : IPerfTestState
        {
            private string name;

            private string action;

            private int linesSinceTitle;

            private double inputRateSum;

            private long inputRowsSum;

            private double outputRateSum;

            private long outputRowsSum;

            private int resultsCount;

            public TestState(string name)
            {
                this.name = name;
                DisplayTitle();
            }

            public string Name
            {
                get => this.name;

                set
                {
                    if (this.name == value)
                    {
                        return;
                    }

                    this.name = value;
                    DisplayTitle();
                }
            }

            public string Action
            {
                get => this.action;

                set
                {
                    if (this.action == value)
                    {
                        return;
                    }

                    this.action = value;
                    DisplayTitle();
                }
            }

            public void AddResult(long inputRows, long outputRows, TimeSpan timing)
            {
                double inputRate = inputRows / timing.TotalSeconds;
                double outputRate = outputRows / timing.TotalSeconds;

                DisplayLines(string.Format(
                    "   RESULT: input rate={0:#,#} rows/sec (rows={1:#,#}), output rate={2:#,#} rows/sec (rows={3:#,#})",
                    inputRate,
                    inputRows,
                    outputRate,
                    outputRows));

                this.inputRowsSum += inputRows;
                this.inputRateSum += inputRate;
                this.outputRowsSum += outputRows;
                this.outputRateSum += outputRate;
                this.resultsCount++;
            }

            public void DisplaySummary()
            {
                if (this.resultsCount > 0)
                {
                    DisplayLines(string.Format(
                        "   ---\n" +
                        "   AVERAGE: input rate={0:#,#} rows/sec (rows={1:#,#}), output rate={2:#,#} rows/sec (rows={3:#,#})", this.inputRateSum / this.resultsCount, this.inputRowsSum / this.resultsCount, this.outputRateSum / this.resultsCount, this.outputRowsSum / this.resultsCount));
                }
                else
                {
                    DisplayLines(string.Format(
                        "   ---\n" +
                        "   AVERAGE: no results collected"));
                }
            }

            public void AddMessage(string message) => DisplayLines("   " + message);

            private static void DisplayOneLine(string text)
            {
                int maxLineLength = Console.WindowWidth;
                if (text.Length < maxLineLength)
                {
                    text = text.PadRight(maxLineLength);
                }
                else if (text.Length > maxLineLength)
                {
                    text = text.Substring(0, maxLineLength - 3) + "...";
                }

                Console.Write(text);
            }

            private static int CountLines(string text)
            {
                int maxLineLength = Console.WindowWidth;
                int lines = 1;
                int lastNewLinePos = -1;
                int newLinePos;
                while ((newLinePos = text.IndexOf('\n', lastNewLinePos + 1)) != -1)
                {
                    int lineLength = newLinePos - lastNewLinePos - 1;
                    lines += (lineLength / maxLineLength) + 1;
                    lastNewLinePos = newLinePos;
                }

                int lastLineLength = text.Length - lastNewLinePos - 1;
                lines += lastLineLength / maxLineLength;
                return lines;
            }

            private void DisplayLines(string text)
            {
                this.linesSinceTitle += CountLines(text);
                Console.WriteLine(text);
            }

            private void DisplayTitle()
            {
                string title = this.action == null
                            ? string.Format("TEST: {0}", this.name)
                            : string.Format("TEST: {0} ({1})", this.name, this.action);

                if (this.linesSinceTitle == 0)
                {
                    DisplayOneLine(title);
                    this.linesSinceTitle = 1;
                }
                else
                {
                    int oldTop = Console.CursorTop;
                    Console.CursorTop = oldTop - this.linesSinceTitle;
                    DisplayOneLine(title);
                    Console.CursorTop = oldTop;
                }
            }
        }
    }
}
