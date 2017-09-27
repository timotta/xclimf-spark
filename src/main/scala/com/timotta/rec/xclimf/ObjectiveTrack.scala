package com.timotta.rec.xclimf

import breeze.linalg.sum
import breeze.linalg.tile
import breeze.linalg.DenseMatrix
import breeze.linalg._
import breeze.numerics._
import breeze.math._
import ScalarMatrixOps._
import breeze.linalg.Axis
import scala.util.Random
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.log4j.LogManager

trait ObjectiveTrack[T] {
  def update(iteractions: Iteractions.Iteractions[T], U: Factors.Factors[T], V: Factors.Factors[T]): Boolean
  def update(iteractions: Iteractions.Iteractions[T], model: XCLiMFModel[T]): Boolean
  def logObjective(iteraction: Int): Unit
}

object ObjectiveTrack {
  def apply[T: ClassTag](shouldTrack: Boolean, ratings: RDD[Rating[T]], lambda: Double, epsilon: Double): ObjectiveTrack[T] = {
    if (shouldTrack) {
      val max = ratings.max()(Ordering.by(_.rating)).rating
      new ObjectiveTrackReal[T](max, lambda, epsilon)
    } else {
      new ObjectiveTrackNull()
    }
  }
}

class ObjectiveTrackReal[T: ClassTag](val maxRating: Double, val lambda: Double, val epsilon: Double)
  extends Serializable with ObjectiveTrack[T] {

  protected[xclimf] var biggerObjective = Double.NegativeInfinity
  protected[xclimf] var lastObjective = Double.NegativeInfinity
  protected[xclimf] var uCount = -1L

  @transient private val logger = LogManager.getLogger(getClass)

  override def update(iteractions: Iteractions.Iteractions[T], U: Factors.Factors[T], V: Factors.Factors[T]): Boolean = {
    val actualObjective = calcAll(iteractions, U, V)
    val result = actualObjective >= biggerObjective - epsilon
    if (actualObjective > biggerObjective) {
      biggerObjective = actualObjective
    }
    lastObjective = actualObjective
    result
  }

  override def update(iteractions: Iteractions.Iteractions[T], model: XCLiMFModel[T]): Boolean = {
    update(iteractions, model.getUserFactors(), model.getItemFactors())
  }

  protected[xclimf] def calcAll(iteractions: Iteractions.Iteractions[T],
    U: Factors.Factors[T], V: Factors.Factors[T]): Double = {
    val errors = iteractions.map {
      case (user, iteraction) => calcOne(iteraction)
    }.sum()
    (errors + regularization(U, V)) / getUCount(U)
  }

  private def getUCount(U: Factors.Factors[T]): Long = {
    if (uCount == -1L) {
      uCount = U.count()
    }
    uCount
  }

  protected[xclimf] def calcOne(iteraction: Iteractions.Iteraction[T]): Double = {
    val N = iteraction.itemNames.size

    val fmiv = iteraction.fmi()
    val fmjv = fmiv.t

    val ymi = iteraction.itemRatings
    val rmi = relevanceProbability(ymi, maxRating)
    val rmj = rmi.t

    val fmi = tile(fmiv, N, 1)
    val fmj = fmi.t
    val fmj_fmi = fmj.:-(fmi)

    val b1 = log(sigmoid(fmiv))
    val rmjGfs = tile(rmj, 1, N).:*(sigmoid(fmj_fmi))
    val log1rg = log(dif(1, rmjGfs))
    val b2 = DenseMatrix(sum(log1rg, Axis._0).t)

    val obj = b1 + b2

    rmi.toDenseVector.dot(obj.toDenseVector)
  }

  private def relevanceProbability(r: DenseMatrix[Double], maxi: Double): DenseMatrix[Double] = {
    div(dif(power(2, r), 1), pow(2, maxi))
  }

  private def regularization(U: Factors.Factors[T], V: Factors.Factors[T]): Double = {
    val sumV2 = V.map { case (_, f) => sum(pow(f, 2)) }.sum()
    val sumU2 = U.map { case (_, f) => sum(pow(f, 2)) }.sum()
    -0.5 * lambda * (sumV2 + sumU2)
  }

  override def toString(): String = {
    "ObjectiveTrackReal(last=" + lastObjective + ")"
  }

  override def logObjective(iteraction: Int): Unit = {
    logger.info("doing iteraction=" + iteraction + ": " + lastObjective)
  }
}

class ObjectiveTrackNull[T: ClassTag]() extends Serializable with ObjectiveTrack[T] {
  override def update(iteractions: Iteractions.Iteractions[T], U: Factors.Factors[T], V: Factors.Factors[T]): Boolean = {
    true
  }
  def update(iteractions: Iteractions.Iteractions[T], model: XCLiMFModel[T]): Boolean = {
    true
  }
  override def logObjective(iteraction: Int): Unit = {
  }
}
