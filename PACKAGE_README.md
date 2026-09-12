# Trill.StreamProcessing

Trill is a high-performance one-pass in-memory streaming analytics engine originally developed by Microsoft Research. It can handle both real-time and offline data, and is based on a temporal data and query model. Trill can be used as a streaming engine, a lightweight in-memory relational engine, and as a progressive query processor for early query results on partial data.

This package is a community-maintained fork of [Microsoft's Trill](https://github.com/microsoft/Trill), updated for .NET 10 and actively maintained by [Amir Burbea](https://github.com/amirburbea).

## Installation

```
dotnet add package Trill.StreamProcessing
```

This package contains `Microsoft.StreamProcessing` (the core streaming engine). The experimental `Microsoft.StreamProcessing.Provider` project in the repo is not published as part of this package.

## Learn More

- [Trill on GitHub](https://github.com/amirburbea/trill)
- [Original announcement blog post](https://azure.microsoft.com/en-us/blog/microsoft-open-sources-trill-to-deliver-insights-on-a-trillion-events-a-day/)
- [Trill paper (VLDB 2015)](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/trill-vldb2015.pdf)
- [Trill samples repository](https://github.com/Microsoft/TrillSamples)
