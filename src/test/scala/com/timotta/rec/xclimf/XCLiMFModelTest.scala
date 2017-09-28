package com.timotta.rec.xclimf

import org.junit.Test
import org.apache.spark.sql.SparkSession
import org.junit.BeforeClass
import org.junit.AfterClass
import scala.util.Random
import org.apache.log4j.PropertyConfigurator
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import org.junit.Assert

object XCLiMFModelTest {

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

class XCLiMFModelTest {

  @Test
  def recommendByCartesian(): Unit = {
    val userFactors = XCLiMFModelTest.spark.sparkContext.parallelize(Seq(
      ("u1", DenseMatrix(DenseVector(0.1, 0.2, 0.6, 1.0))),
      ("u2", DenseMatrix(DenseVector(0.9, 0.4, 0.2, 0.3))),
      ("u3", DenseMatrix(DenseVector(0.4, 0.8, 0.5, 0.4))),
      ("u4", DenseMatrix(DenseVector(0.4, 0.1, 0.8, 0.1)))))

    val itemFactors = XCLiMFModelTest.spark.sparkContext.parallelize(Seq(
      ("i1", DenseMatrix(DenseVector(0.5, 0.2, 0.3, 0.2))),
      ("i2", DenseMatrix(DenseVector(0.8, 0.9, 0.1, 0.1))),
      ("i3", DenseMatrix(DenseVector(0.4, 0.9, 0.5, 0.3))),
      ("i4", DenseMatrix(DenseVector(0.4, 0.2, 0.1, 0.9)))))

    val model = new XCLiMFModel(userFactors, itemFactors)
    val result = model.recommend(2, 2).collect().toMap

    val expected = Map(
      "u4" -> List(("i3", 0.68), ("i2", 0.50)),
      "u2" -> List(("i2", 1.13), ("i3", 0.91)),
      "u3" -> List(("i3", 1.25), ("i2", 1.13)),
      "u1" -> List(("i4", 1.04), ("i3", 0.82)))

    expected.foreach {
      case (k, v) =>
        Assert.assertEquals(2, v.size)
        0.until(2).foreach { i =>
          Assert.assertEquals(v(i)._1, result(k)(i)._1)
          Assert.assertEquals(v(i)._2, result(k)(i)._2, 0.01)
        }
    }
  }

  @Test
  def recommendByBroadcast(): Unit = {
    val userFactors = XCLiMFModelTest.spark.sparkContext.parallelize(Seq(
      ("u1", DenseMatrix(DenseVector(0.1, 0.2, 0.6, 1.0))),
      ("u2", DenseMatrix(DenseVector(0.9, 0.4, 0.2, 0.3))),
      ("u3", DenseMatrix(DenseVector(0.4, 0.8, 0.5, 0.4))),
      ("u4", DenseMatrix(DenseVector(0.4, 0.1, 0.8, 0.1)))))

    val itemFactors = XCLiMFModelTest.spark.sparkContext.parallelize(Seq(
      ("i1", DenseMatrix(DenseVector(0.5, 0.2, 0.3, 0.2))),
      ("i2", DenseMatrix(DenseVector(0.8, 0.9, 0.1, 0.1))),
      ("i3", DenseMatrix(DenseVector(0.4, 0.9, 0.5, 0.3))),
      ("i4", DenseMatrix(DenseVector(0.4, 0.2, 0.1, 0.9)))))

    val model = new XCLiMFModel(userFactors, itemFactors)
    val result = model.recommend(2, 10).collect().toMap

    val expected = Map(
      "u4" -> List(("i3", 0.68), ("i2", 0.50)),
      "u2" -> List(("i2", 1.13), ("i3", 0.91)),
      "u3" -> List(("i3", 1.25), ("i2", 1.13)),
      "u1" -> List(("i4", 1.04), ("i3", 0.82)))

    expected.foreach {
      case (k, v) =>
        Assert.assertEquals(2, v.size)
        0.until(2).foreach { i =>
          Assert.assertEquals(v(i)._1, result(k)(i)._1)
          Assert.assertEquals(v(i)._2, result(k)(i)._2, 0.01)
        }
    }
  }

  @Test
  def recommendIgnoring(): Unit = {
    val userFactors = XCLiMFModelTest.spark.sparkContext.parallelize(Seq(
      ("u1", DenseMatrix(DenseVector(0.1, 0.2, 0.6, 1.0))),
      ("u2", DenseMatrix(DenseVector(0.9, 0.4, 0.2, 0.3))),
      ("u3", DenseMatrix(DenseVector(0.4, 0.8, 0.5, 0.4))),
      ("u4", DenseMatrix(DenseVector(0.4, 0.1, 0.8, 0.1)))))

    val itemFactors = XCLiMFModelTest.spark.sparkContext.parallelize(Seq(
      ("i1", DenseMatrix(DenseVector(0.5, 0.2, 0.3, 0.2))),
      ("i2", DenseMatrix(DenseVector(0.8, 0.9, 0.1, 0.1))),
      ("i3", DenseMatrix(DenseVector(0.4, 0.9, 0.5, 0.3))),
      ("i4", DenseMatrix(DenseVector(0.4, 0.2, 0.1, 0.9)))))

    val ignoring = XCLiMFModelTest.spark.sparkContext.parallelize(Seq(
      ("u2", Set("i2")),
      ("u3", Set("i3"))))

    val model = new XCLiMFModel(userFactors, itemFactors)
    val result = model.recommend(2, ignoring).collect().toMap

    val expected = Map(
      "u4" -> List(("i3", 0.68), ("i2", 0.50)),
      "u2" -> List(("i3", 0.91), ("i4", 0.73)),
      "u3" -> List(("i2", 1.13), ("i4", 0.73)),
      "u1" -> List(("i4", 1.04), ("i3", 0.82)))

    expected.foreach {
      case (k, v) =>
        Assert.assertEquals(2, v.size)
        0.until(2).foreach { i =>
          Assert.assertEquals(v(i)._1, result(k)(i)._1)
          Assert.assertEquals(v(i)._2, result(k)(i)._2, 0.01)
        }
    }
  }

  @Test
  def recommendFiltering(): Unit = {
    val userFactors = XCLiMFModelTest.spark.sparkContext.parallelize(Seq(
      ("u1", DenseMatrix(DenseVector(0.1, 0.2, 0.6, 1.0))),
      ("u2", DenseMatrix(DenseVector(0.9, 0.4, 0.2, 0.3))),
      ("u3", DenseMatrix(DenseVector(0.4, 0.8, 0.5, 0.4))),
      ("u4", DenseMatrix(DenseVector(0.4, 0.1, 0.8, 0.1)))))

    val itemFactors = XCLiMFModelTest.spark.sparkContext.parallelize(Seq(
      ("i1", DenseMatrix(DenseVector(0.5, 0.2, 0.3, 0.2))),
      ("i2", DenseMatrix(DenseVector(0.8, 0.9, 0.1, 0.1))),
      ("i3", DenseMatrix(DenseVector(0.4, 0.9, 0.5, 0.3))),
      ("i4", DenseMatrix(DenseVector(0.4, 0.2, 0.1, 0.9)))))

    val itemFilter = XCLiMFModelTest.spark.sparkContext.parallelize(Seq("i2", "i4", "i1"))

    val model = new XCLiMFModel(userFactors, itemFactors)
    val result = model.recommend(2, Some(itemFilter)).collect().toMap

    val expected = Map(
      "u4" -> List(("i2", 0.50), ("i1", 0.48)),
      "u2" -> List(("i2", 1.13), ("i4", 0.73)),
      "u3" -> List(("i2", 1.13), ("i4", 0.73)),
      "u1" -> List(("i4", 1.04), ("i1", 0.47)))

    expected.foreach {
      case (k, v) =>
        Assert.assertEquals(2, v.size)
        0.until(2).foreach { i =>
          Assert.assertEquals(v(i)._1, result(k)(i)._1)
          Assert.assertEquals(v(i)._2, result(k)(i)._2, 0.01)
        }
    }
  }

}
