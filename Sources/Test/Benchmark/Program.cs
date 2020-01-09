using System;
using System.Linq;
using System.Threading;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Microsoft.StreamProcessing;

namespace Benchmark
{
    public class AvgTest
    {
        public void TestScript1()
        {
            Console.WriteLine("Before");
            Thread.Sleep(100);
            Console.WriteLine("After");
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting");
            Config.ForceRowBasedExecution = true;

            if (args.Length > 0 && args[0] == "-DisableNewOptimizations")
            {
                Console.WriteLine("Disabling New Optimizations");
                Config.DisableNewOptimizations = true;
                args = args.Skip(1).ToArray();
            }

            /* Another way to run:
               var config = DefaultConfig.Instance.With(Job.Default.WithWarmupCount(1).WithIterationCount(1)..AsDefault());
               BenchmarkRunner.Run<HoppingWindowTopKAggregateBenchmark>(config);
            */

            BenchmarkSwitcher.FromAssembly(typeof(HoppingWindowTopKAggregateBenchmark).Assembly).Run(args);
        }
    }
}
