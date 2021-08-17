Quick Start
-----
This tutorial provides instructions to help you get started with Kotlin Spark API.

## Supported versions of Apache Spark

| Apache Spark | Scala |  Kotlin for Apache Spark |
|:------------:|:-----------:|:------------:|
| 3.0.0+        | 2.12 | kotlin-spark-api-3.0.0:1.0.0-preview2    |
| 2.4.1+        | 2.12 | kotlin-spark-api-2.4_2.12:1.0.0-preview2 |
| 2.4.1+        | 2.11 | kotlin-spark-api-2.4_2.11:1.0.0-preview2 |

## Releases

The list of Kotlin for Apache Spark releases is available [here](https://github.com/JetBrains/kotlin-spark-api/releases/).
The Kotlin for Spark artifacts adhere to the following convention:
`[Apache Spark version]_[Scala core version]:[Kotlin for Apache Spark API version]` 

[![Maven Central](https://img.shields.io/maven-central/v/org.jetbrains.kotlinx.spark/kotlin-spark-api-parent.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.jetbrains.kotlinx.spark%22%20AND%20a:%22kotlin-spark-api-3.0.0_2.12%22)

## Kotlin for Apache Spark features

### Creating a SparkSession in Kotlin
```kotlin
val spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("Simple Application").orCreate

```

### Creating a Dataset in Kotlin
```kotlin
spark.toDS("a" to 1, "b" to 2)
```
The example above produces `Dataset<Pair<String, Int>>`.
 
### Null safety
There are several aliases in API, like `leftJoin`, `rightJoin` etc. These are null-safe by design. 
For example, `leftJoin` is aware of nullability and returns `Dataset<Pair<LEFT, RIGHT?>>`.
Note that we are forcing `RIGHT` to be nullable for you as a developer to be able to handle this situation. 
`NullPointerException`s are hard to debug in Spark, and we doing our best to make them as rare as possible.

### withSpark function

We provide you with useful function `withSpark`, which accepts everything that may be needed to run Spark — properties, name, master location and so on. It also accepts a block of code to execute inside Spark context.

After work block ends, `spark.stop()` is called automatically.

```kotlin
withSpark {
    dsOf(1, 2)
            .map { it to it }
            .show()
}
```

`dsOf` is just one more way to create `Dataset` (`Dataset<Int>`) from varargs.

### withCached function
It can easily happen that we need to fork our computation to several paths. To compute things only once we should call `cache`
method. However, it becomes difficult to control when we're using cached `Dataset` and when not.
It is also easy to forget to unpersist cached data, which can break things unexpectedly or take up more memory
than intended.

To solve these problems we've added `withCached` function

```kotlin
withSpark {
    dsOf(1, 2, 3, 4, 5)
            .map { it to (it + 2) }
            .withCached {
                showDS()

                filter { it.first % 2 == 0 }.showDS()
            }
            .map { c(it.first, it.second, (it.first + it.second) * 2) }
            .show()
}
```

Here we're showing cached `Dataset` for debugging purposes then filtering it. 
The `filter` method returns filtered `Dataset` and then the cached `Dataset` is being unpersisted, so we have more memory t
o call the `map` method and collect the resulting `Dataset`.

### toList and toArray methods

For more idiomatic Kotlin code we've added `toList` and `toArray` methods in this API. You can still use the `collect` method as in Scala API, however the result should be casted to `Array`.
  This is because `collect` returns a Scala array, which is not the same as Java/Kotlin one.

Links
-----
* JetBrains: https://blog.jetbrains.com/kotlin/2020/08/introducing-kotlin-for-apache-spark-preview/
* Kotlin-Jupyter: https://github.com/Kotlin/kotlin-jupyter