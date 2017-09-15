package com.timotta.rec.xclimf

import org.junit.Test
import org.apache.spark.sql.SparkSession
import org.junit.BeforeClass
import org.junit.AfterClass
import org.junit.Assert

object RatingTest {

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

class RatingTest {

  @Test
  def prepareShouldIgnoreGlobalTopK() = {
    val ratings = RatingTest.spark.sparkContext.parallelize(Seq(
      Rating[String]("u1", "i1", 5D),
      Rating[String]("u2", "i2", 6D),
      Rating[String]("u3", "i3", 7D),
      Rating[String]("u4", "i1", 2D),
      Rating[String]("u5", "i4", 4D)))
    val items = Rating.prepare(ratings, 2).map(_.item).collect().toSet
    val expected = Set("i2", "i4")
    Assert.assertEquals(expected, items)
  }

  @Test
  def prepareShouldNormalizeRatings() = {
    val ratings = RatingTest.spark.sparkContext.parallelize(Seq(
      Rating[String]("u1", "i1", 5D),
      Rating[String]("u2", "i2", 4D),
      Rating[String]("u3", "i3", 3D),
      Rating[String]("u4", "i1", 2D),
      Rating[String]("u5", "i4", 1D)))
    val result = Rating.prepare(ratings, 0).map { r => (r.user, r.rating) }.collect().toMap

    Assert.assertEquals(1D, result("u1"), 0.01)
    Assert.assertEquals(.8, result("u2"), 0.01)
    Assert.assertEquals(.6, result("u3"), 0.01)
    Assert.assertEquals(.4, result("u4"), 0.01)
    Assert.assertEquals(.2, result("u5"), 0.01)
  }

  @Test
  def prepareShouldNormalizeRatingsMin0() = {
    val ratings = RatingTest.spark.sparkContext.parallelize(Seq(
      Rating[String]("u1", "i1", 5D),
      Rating[String]("u2", "i2", 4D),
      Rating[String]("u3", "i3", 3D),
      Rating[String]("u4", "i1", 2D),
      Rating[String]("u5", "i4", 0D)))
    val result = Rating.prepare(ratings, 0).map { r => (r.user, r.rating) }.collect().toMap

    Assert.assertEquals(1D, result("u1"), 0.01)
    Assert.assertEquals(.83, result("u2"), 0.01)
    Assert.assertEquals(.66, result("u3"), 0.01)
    Assert.assertEquals(.5, result("u4"), 0.01)
    Assert.assertEquals(.16, result("u5"), 0.01)
  }

  @Test
  def prepareShouldNormalizeRatingsMinNegative() = {
    val ratings = RatingTest.spark.sparkContext.parallelize(Seq(
      Rating[String]("u1", "i1", 2D),
      Rating[String]("u2", "i2", 1D),
      Rating[String]("u3", "i3", 0D),
      Rating[String]("u4", "i1", -1D),
      Rating[String]("u5", "i4", -2D)))
    val result = Rating.prepare(ratings, 0).map { r => (r.user, r.rating) }.collect().toMap

    Assert.assertEquals(1D, result("u1"), 0.01)
    Assert.assertEquals(.8, result("u2"), 0.01)
    Assert.assertEquals(.6, result("u3"), 0.01)
    Assert.assertEquals(.4, result("u4"), 0.01)
    Assert.assertEquals(.2, result("u5"), 0.01)
  }

}
