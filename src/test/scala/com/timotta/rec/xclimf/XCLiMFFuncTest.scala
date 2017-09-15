package com.timotta.rec.xclimf

import org.junit.Test
import org.apache.spark.sql.SparkSession
import org.junit.BeforeClass
import org.junit.AfterClass
import scala.util.Random
import org.apache.log4j.PropertyConfigurator

object XCLiMFFuncTest {

  var spark: SparkSession = _

  @BeforeClass
  def before(): Unit = {
    spark = SparkSession.builder().master("local").getOrCreate()
  }

  @AfterClass
  def after(): Unit = {
    spark.stop()
  }
}

class XCLiMFFuncTest {

  @Test
  def fitNotError(): Unit = {
    val rdd = XCLiMFFuncTest.spark.sparkContext.parallelize(0.to(1000).map { i =>
      val user = "user" + Random.nextInt(100)
      val item = "item" + Random.nextInt(40)
      val rating = Random.nextDouble()
      Rating[String](user, item, rating)
    }.toSeq).keyBy { r => (r.user, r.item) }.reduceByKey((a,b) => if(a.rating>b.rating) a else b).values
    val xclimf = new XCLiMF[String](maxIters=5)
    val model = xclimf.fit(rdd)
    model.getItemFactors().take(10).foreach(println(_))
    model.getUserFactors().take(10).foreach(println(_))
  }

}
