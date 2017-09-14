package com.timotta.rec.xclimf

import org.apache.spark.sql.SparkSession
import org.apache.log4j.PropertyConfigurator
import scala.util.Random

object Main extends App {

  PropertyConfigurator.configure(getClass().getResource("/log4j-timotta.properties"))

  val spark = SparkSession.builder().master("local[2]").appName("xCLiMF").getOrCreate()

  val rdd = spark.sparkContext.parallelize(0.to(1000).map { i =>
    val user = "user" + Random.nextInt(100)
    val item = "item" + Random.nextInt(40)
    val rating = Random.nextDouble()
    Rating[String](user, item, rating)
  }.toSeq).keyBy { r => (r.user, r.item) }.reduceByKey((a,b) => if(a.rating>b.rating) a else b).values

  val xclimf = new XCLiMF[String]()
  xclimf.fit(rdd)
}
