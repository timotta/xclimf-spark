Spark xCLiMF
============

Spark implementation of the Extended Collaborative Less-isMore Filtering based
in the [python xCLiMF implementation](https://github.com/timotta/xclimf). xCLiMF 
is a matrix factorization algorithm to optimize recommendation ranked lists. 

## Using

Example: [Main.scala](https://github.com/timotta/xclimf-spark/blob/master/src/main/scala/com/timotta/rec/xclimf/Main.scala)

## Installation

Using gradle:

```
compile 'com.timotta:xclimf-spark:0.0.2'
```

Using maven:

```xml
<dependency>
  <groupId>com.timotta</groupId>
  <artifactId>xclimf-spark</artifactId>
  <version>0.0.1</version>
</dependency>
```

## Requirements

- Java 8
- Scala 2.11.*
- Spark 2.1.1

## Contribute

To test

```
gradle clean test
```

To run main with spark

```
gradle clean jar
$SPARK_HOME/bin/spark-submit --master local --class com.timotta.rec.xclimf.Main build/libs/xclimf-spark.jar
```

To upload new package

```
gradle -Prelease uploadArchives
```

To release

```
gradle -Prelease closeAndReleaseRepository
```



