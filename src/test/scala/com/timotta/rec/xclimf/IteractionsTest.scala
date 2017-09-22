package com.timotta.rec.xclimf

import org.apache.spark.sql.SparkSession
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import org.junit.Assert

object IteractionsTest {

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

class IteractionsTest {
  @Test
  def prepare(): Unit = {
    val users = IteractionsTest.spark.sparkContext.parallelize(Seq(
      ("u1", Array(("i1", 1D), ("i2", 2D))),
      ("u2", Array(("i2", 2D), ("i3", 3D)))))

    val ratings = Rating.flatByItems(users)

    val userFactors = IteractionsTest.spark.sparkContext.parallelize(Seq(
      ("u1", DenseMatrix(DenseVector(1D, 1.1, 1.2, 1.3))),
      ("u2", DenseMatrix(DenseVector(2D, 2.1, 2.2, 2.3)))))

    val itemFactors = IteractionsTest.spark.sparkContext.parallelize(Seq(
      ("i1", DenseMatrix(DenseVector(9.1, 8.1, 7.1, 6.1))),
      ("i2", DenseMatrix(DenseVector(9.2, 8.2, 7.2, 6.2))),
      ("i3", DenseMatrix(DenseVector(9.3, 8.3, 7.3, 6.3)))))

    val results = Iteractions.prepare(ratings, 2, userFactors, itemFactors).collect().toMap

    Assert.assertEquals(Set("i1", "i2"), results("u1").itemNames.toSet)
    Assert.assertEquals(DenseMatrix(DenseVector(1D, 1.1, 1.2, 1.3)), results("u1").userFactors)
    0.until(2).foreach { i =>
      val itemName = results("u1").itemNames(i)
      val imap = itemFactors.collect().toMap
      Assert.assertEquals( imap(itemName).toArray.toList, results("u1").itemFactors(i,::).t.toArray.toList )
      val umap = users.collect().toMap.apply("u1").toMap
      Assert.assertEquals( umap(itemName), results("u1").itemRatings.toArray(i), 0.1)
    }

    Assert.assertEquals(Set("i2", "i3"), results("u2").itemNames.toSet)
    Assert.assertEquals(DenseMatrix(DenseVector(2D, 2.1, 2.2, 2.3)), results("u2").userFactors)
    0.until(2).foreach { i =>
      val itemName = results("u2").itemNames(i)
      val imap = itemFactors.collect().toMap
      Assert.assertEquals( imap(itemName).toArray.toList, results("u2").itemFactors(i,::).t.toArray.toList )
      val umap = users.collect().toMap.apply("u2").toMap
      Assert.assertEquals( umap(itemName), results("u2").itemRatings.toArray(i), 0.1)
    }
  }
}
