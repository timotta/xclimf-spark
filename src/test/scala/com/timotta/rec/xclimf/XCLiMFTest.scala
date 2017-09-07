package com.timotta.rec.xclimf

import org.junit.Test
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix
import breeze.linalg._
import breeze.numerics._
import org.junit.Assert

class XCLiMFTest {

  @Test
  def updateOneUser2x4(): Unit = {

    val xclimf = new XCLiMF[String](dims = 4, lambda = 0.1, gamma = 0.1)

    val result = xclimf.updateOneUser("u1", Iteractions.Iteraction(
      userFactors = DenseMatrix(DenseVector(0.02, 0.01, 0.03, 0.04)),
      itemNames = List("i1", "i2"),
      itemRatings = DenseMatrix(DenseVector(0.2D, 0.5D)),
      itemFactors = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.015),
        DenseVector(0.05, 0.07, 0.11, 0.101))))

    val userFactorsExpected = List(0.02112183,  0.01181387,  0.03269244,  0.04221523)

    0.to(1).foreach { i =>
      Assert.assertEquals(userFactorsExpected(i), result.userFactors(0, i), 1e-8)
    }

    val itemsFactorsExpected = List(
        List(1.11022424e-04,  -9.44887880e-05,   1.65336360e-05, 2.72044848e-04),
        List(-1.34724085e-05,  -4.56736204e-04,  -3.70208613e-04, -3.69448171e-05))

    0.to(1).foreach { i =>
      0.to(2).foreach { j =>
        Assert.assertEquals(itemsFactorsExpected(i)(j), result.itemFactors(i,j), 1e-9)
      }
    }
  }

  @Test
  def updateOneUser3x4(): Unit = {
    val xclimf = new XCLiMF[String](dims = 4, lambda = 0.1, gamma = 0.1)

    val result = xclimf.updateOneUser("u1", Iteractions.Iteraction(
      userFactors = DenseMatrix(DenseVector(0.02, 0.01, 0.03, 0.04)),
      itemNames = List("i1", "i2", "i3"),
      itemRatings = DenseMatrix(DenseVector(0.2D, 0.5D, 0.7D)),
      itemFactors = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.015),
        DenseVector(0.05, 0.07, 0.11, 0.101),
        DenseVector(0.02, 0.01, 0.01, 0.03))))

    val userFactorsExpected = List(0.02185884,  0.01228382,  0.03324757,  0.0433664)

    0.to(2).foreach { i =>
      Assert.assertEquals(userFactorsExpected(i), result.userFactors(0, i), 1e-8)
    }

    val itemsFactorsExpected = List(
        List(1.40940285e-04,  -7.95298573e-05,   6.14104281e-05, 3.31880571e-04),
        List(2.16411657e-05,  -4.39179417e-04,  -3.17538252e-04, 3.32823313e-05),
        List(4.34268565e-04,   2.17134282e-04,   8.51402847e-04, 9.68537129e-04))

    0.to(2).foreach { i =>
      0.to(3).foreach { j =>
        Assert.assertEquals(itemsFactorsExpected(i)(j), result.itemFactors(i,j), 1e-9)
      }
    }
  }
}
