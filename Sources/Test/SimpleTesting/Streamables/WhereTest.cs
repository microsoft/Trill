using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class WhereTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void WhereClauseDoesNotProcessSubsequentExpressions()
        {
            var data = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(0, 100, "1"),
                StreamEvent.CreateInterval(1, 100, "2"),
                StreamEvent.CreateInterval(2, 100, "NA"),
                StreamEvent.CreateInterval(3, 100, "3"),
            }.ToObservable()
             .ToStreamable();

            var result = data.Where(x => x != "NA") // First, filter out invalid values
                             .Where(x => int.Parse(x) > 1); // Then, work with valid ints

            var expected = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(1, 100, "2"),
                StreamEvent.CreateInterval(3, 100, "3"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(result.IsEquivalentTo(expected));
        }
    }
}
