package com.timotta.rec.xclimf

import org.junit.Test
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix
import org.junit.Assert
import org.mockito.Mockito._
import org.mockito.Matchers._

class ObjectiveTrackTest {

  @Test
  def updateWhenCalcBigger(): Unit = {
    val objectiveTrack = spy(new ObjectiveTrackReal[String](0D, 0D, epsilon=1D))
    objectiveTrack.biggerObjective = 8D
    doReturn(10D).when(objectiveTrack).calcAll(any(), any(), any())

    Assert.assertTrue( objectiveTrack.update(null, null, null) )
    Assert.assertEquals( 10D, objectiveTrack.biggerObjective, 0.1 )
  }

  @Test
  def updateWhenCalcEqualButInTheEpsilonMargin(): Unit = {
    val objectiveTrack = spy(new ObjectiveTrackReal[String](0D, 0D, epsilon=1D))
    objectiveTrack.biggerObjective = 10D
    doReturn(10D).when(objectiveTrack).calcAll(any(), any(), any())

    Assert.assertTrue( objectiveTrack.update(null, null, null) )
    Assert.assertEquals( 10D, objectiveTrack.biggerObjective, 0.1 )
  }

  @Test
  def updateWhenCalcSmallerButInTheEpsilonMargin(): Unit = {
    val objectiveTrack = spy(new ObjectiveTrackReal[String](0D, 0D, epsilon=1D))
    objectiveTrack.biggerObjective = 10D
    doReturn(9D).when(objectiveTrack).calcAll(any(), any(), any())

    Assert.assertTrue( objectiveTrack.update(null, null, null) )
    Assert.assertEquals( 10D, objectiveTrack.biggerObjective, 0.1 )
  }

  @Test
  def updateWhenCalcSmaller(): Unit = {
    val objectiveTrack = spy(new ObjectiveTrackReal[String](0D, 0D, epsilon=1D))
    objectiveTrack.biggerObjective = 10D
    doReturn(8D).when(objectiveTrack).calcAll(any(), any(), any())

    Assert.assertFalse( objectiveTrack.update(null, null, null) )
    Assert.assertEquals( 10D, objectiveTrack.biggerObjective, 0.1 )
  }

  @Test
  def calcOne2x4(): Unit = {
    val iteraction = Iteractions.Iteraction(
      userFactors = DenseMatrix(DenseVector(0.02, 0.01, 0.03, 0.04)),
      itemNames = List("i1", "i3"),
      itemRatings = DenseMatrix(DenseVector(0.2D, 0.5D)),
      itemFactors = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.15),
        DenseVector(0.05, 0.07, 0.11, 0.101)))

    val objectiveTrack = new ObjectiveTrackReal[String](maxRating = 0.5D, lambda = 0.1, epsilon = 1e-4)

    val objective = objectiveTrack.calcOne(iteraction)

    Assert.assertEquals(-0.35872157, objective, 1e-8)
  }

  @Test
  def calcOne3x4(): Unit = {
    val iteraction = Iteractions.Iteraction(
      userFactors = DenseMatrix(DenseVector(0.02, 0.01, 0.03, 0.04)),
      itemNames = List("i1", "i2", "i3"),
      itemRatings = DenseMatrix(DenseVector(0.2, 0.5, 0.7)),
      itemFactors = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.015),
        DenseVector(0.05, 0.07, 0.11, 0.101),
        DenseVector(0.02, 0.01, 0.01, 0.03)))

    val objectiveTrack = new ObjectiveTrackReal[String](maxRating = 0.7D, lambda = 0.1, epsilon = 1e-4)

    val objective = objectiveTrack.calcOne(iteraction)

    Assert.assertEquals(-0.794970905, objective, 1e-8)
  }

  @Test
  def calcOne5x10(): Unit = {
    val iteraction = Iteractions.Iteraction(
      userFactors = DenseMatrix(DenseVector(0.02, 0.01, 0.03, 0.04, 0.05, 0.02, 0.02, 0.03, 0.01, 0.07)),
      itemNames = List("i1", "i2", "i3", "i4", "i5"),
      itemRatings = DenseMatrix(DenseVector(0.2, 0.5, 0.7, 0.3, 0.8)),
      itemFactors = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.015, 0.04, 0.03, 0.02, 0.01, 0.009, 0.012),
        DenseVector(0.05, 0.07, 0.11, 0.101, 0.01, 0.02, 0.03, 0.04, 0.01,  0.001),
        DenseVector(0.02, 0.01, 0.03, 0.03,  0.02, 0.03, 0.04, 0.03, 0.02,  0.01),
        DenseVector(0.02, 0.02, 0.01, 0.03,  0.02, 0.03, 0.05, 0.03, 0.02,  0.01),
        DenseVector(0.02, 0.01, 0.08, 0.03,  0.01, 0.06, 0.02, 0.031, 0.021,  0.02)))

    val objectiveTrack = new ObjectiveTrackReal[String](maxRating = 0.8D, lambda = 0.1, epsilon = 1e-4)

    val objective = objectiveTrack.calcOne(iteraction)

    Assert.assertEquals(-1.69301826150723, objective, 1e-8)
  }

}
