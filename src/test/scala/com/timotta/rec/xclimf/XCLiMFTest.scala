package com.timotta.rec.xclimf

import org.junit.Test
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix
import breeze.linalg._
import breeze.numerics._
import org.junit.Assert

class XCLiMFTest {

  @Test
  def updateOneUser1(): Unit = {

    val xclimf = new XCLiMF[String](dims = 4, lambda = 0.1, gamma = 0.1)

    xclimf.updateOneUser("u1", Iteractions.Iteraction(
      userFactors = DenseVector(0.02, 0.01, 0.03, 0.04),
      itemNames = List("i1", "i2"),
      itemRatings = DenseVector(0.2f, 0.5f),
      itemFactors = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.015),
        DenseVector(0.05, 0.07, 0.11, 0.101))))

  }

  @Test
  def updateOneUser2(): Unit = {

    val xclimf = new XCLiMF[String](dims = 4, lambda = 0.1, gamma = 0.1)

    xclimf.updateOneUser("u1", Iteractions.Iteraction(
      userFactors = DenseVector(0.02, 0.01, 0.03, 0.04),
      itemNames = List("i1", "i2", "i3"),
      itemRatings = DenseVector(0.2f, 0.5f, 0.7f),
      itemFactors = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.015),
        DenseVector(0.05, 0.07, 0.11, 0.101),
        DenseVector(0.02, 0.01, 0.01, 0.03))))

  }
}
