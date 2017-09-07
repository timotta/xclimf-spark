package com.timotta.rec.xclimf

import org.junit.Test
import org.junit.Assert
import org.junit.BeforeClass
import org.apache.spark.sql.SparkSession
import org.junit.AfterClass
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix
import org.apache.spark.ml.recommendation.ALS.Rating

object FactorsTest {

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

class FactorsTest {

  @Test
  def startUserFactors(): Unit = {
    val users = FactorsTest.spark.sparkContext.parallelize(Seq(
      ("u1", Array(("i1", 1f), ("i2", 2f))),
      ("u2", Array(("i1", 1f), ("i2", 2f)))
    ))

    val result = Factors.startUserFactors(users, 4).collect().toMap

    Assert.assertEquals(4, result("u1").size)
    Assert.assertEquals(4, result("u2").size)
    0.to(3).foreach { i =>
      Assert.assertTrue( result("u1")(i) < 0.1 )
      Assert.assertTrue( result("u2")(i) < 0.1 )
    }
  }

  @Test
  def startItemFactors(): Unit = {
    val ratings = FactorsTest.spark.sparkContext.parallelize(Seq(
      Rating[String]("u1", "i1", 10f),
      Rating[String]("u2", "i2", 10f),
      Rating[String]("u3", "i1", 10f)
    ))

    val result = Factors.startItemFactors(ratings, 5).collect().toMap

    Assert.assertEquals(2, result.size)
    Assert.assertEquals(5, result("i1").size)
    Assert.assertEquals(5, result("i2").size)
    0.to(4).foreach { i =>
      Assert.assertTrue( result("i1")(i) < 0.1 )
      Assert.assertTrue( result("i2")(i) < 0.1 )
    }
  }

  @Test
  def asItemFactors(): Unit = {
    val iteractions = FactorsTest.spark.sparkContext.parallelize(Seq(
      ("user1", Iteractions.Iteraction(
          DenseVector(1.0), List("a", "b"), DenseVector(1.0), DenseMatrix(List(1.0, 2.0, 3.0), List(5.0, 7.0, 11.0)) )),
      ("user2", Iteractions.Iteraction(
          DenseVector(1.0), List("b", "c"), DenseVector(1.0), DenseMatrix(List(5.0, 7.0, 11.0), List(3.0, 2.0, 1.0)) ))
    ))

    val itemFactors = FactorsTest.spark.sparkContext.parallelize(Seq(
       ("a", DenseVector(0.1, 0.2, 0.3)),
       ("b", DenseVector(0.5, 0.7, 0.11)),
       ("c", DenseVector(0.13, 0.17, 0.19))
    ))

    val result = Factors.asItemFactors(iteractions, itemFactors).collect().toMap

    Assert.assertEquals(DenseVector(1.1, 2.2, 3.3), result("a"))
    Assert.assertEquals(DenseVector(10.5, 14.7, 22.11), result("b"))
    Assert.assertEquals(DenseVector(3.13, 2.17, 1.19), result("c"))
  }

}
