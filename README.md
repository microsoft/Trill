
# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

# Introduction

Trill is a high-performance one-pass in-memory streaming analytics engine from Microsoft Research. It can handle both real-time and offline data, and is based on a temporal data and query model. Trill can be used as a streaming engine, a lightweight in-memory relational engine, and as a progressive query processor (for early query results on partial data).

# Getting Started

1. Of course, the sources are right here!
2. You can get binaries from our [NuGet feed](https://www.nuget.org/packages/Trill/).
3. You can also check out samples of Trill usage at our [samples repository](https://github.com/Microsoft/TrillSamples).

# Learn More

- The [Trill paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/trill-vldb2015.pdf) appeared at VLDB.
- An [article](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/trill-debull.pdf) on Trill appeared in the IEEE Data Engineering Bulletin.
- The [Trill technical report](http://research.microsoft.com/pubs/214609/trill-TR.pdf).
- Additional documents located in the /Documentation directory:
    - TrillQueryWritingGuide: introduces basic concepts of Trill query authoring
    - Ingress: describes how data is ingressed into and egressed out of Trill
    - TrillInternals: outlines Trill innovations and internals
    - BestPractices: describes best practices low-memory real-time deployments 
    - HighAvailability: details Trill high-availability support
    - UserDefinedAggregates: introduces a framework for query authors to create custom aggregates
