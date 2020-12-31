## Iskra

> **Iskra**: (Polish) The diminutive form of "spark"

Iskra is local-only runtime for Apache Spark SQL, optimized for **small** data.

## Dear god, why?

Spark has a lot to offer small data workloads (examples follow), but has a HUGE startup cost (cold queries on a few dozen rows can take upwards of 10 seconds).  The intent of Iskra is to provide low-overhead access to most of the features offered by Spark, while retaining the same comfortable programming environment.

#### Infrastructure re-use
Spark is a fantastic JVM-based environment for machine learning and big-data processing. However, in getting to this point it's also built up a ton of infrastructure around itself, including:

1. Support for reading all sorts of formats like Excel and Google spreadsheets
2. A complex typesystem involving hierarchical types like Structures and Arrays
3. Streamlined support for UDFs in Scala, Python, etc...

#### Automatic scaling
By using the same programming metaphors, you can rapidly iterate on a small sample of your data and immediately run the same code on a larger dataset deployed to a full Spark cluster.

#### Precedent
See [GraphChi](https://github.com/GraphChi/)

