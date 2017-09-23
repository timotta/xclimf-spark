package com.timotta.rec.xclimf

import org.apache.spark.sql.SparkSession
import org.apache.log4j.PropertyConfigurator
import scala.util.Random

object Main extends App {

  PropertyConfigurator.configure(getClass().getResource("/log4j-timotta.properties"))

  val spark = SparkSession.builder().master("local[4]").appName("xCLiMF").getOrCreate()

  val rdd = spark.sparkContext.parallelize(0.to(50000).map { i =>
    val user = "user" + Random.nextInt(10000)
    val item = "item" + Random.nextInt(400)
    val rating = Random.nextDouble()
    Rating[String](user, item, rating)
  }.toSeq).keyBy { r => (r.user, r.item) }.reduceByKey((a,b) => if(a.rating>b.rating) a else b).values

  val xclimf = new XCLiMF[String]()
  val model = xclimf.fit(rdd)

  model.getItemFactors().take(10).foreach(println(_))
  model.getUserFactors().take(10).foreach(println(_))

  model.recommend(5).take(10).foreach { a =>
    println(a._1 + " => " + a._2.toList)
  }
}
