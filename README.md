TODO
====

- Publish spark package
- Experiment with bigdata
- Predict function

## Spark Version

Tested in Spark 2.1.1

## Running testing Main

```
gradle clean jar
$SPARK_HOME/bin/spark-submit --master local --class com.timotta.rec.xclimf.Main build/libs/xclimf-spark.jar
```

