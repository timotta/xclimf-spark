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

class ObjectiveTrack[T](val maxRating: Double, val lambda: Double, val epsilon: Double) extends Serializable {

  var biggerObjective = Double.NegativeInfinity
  var lastObjective = Double.NegativeInfinity

  def update(iteractions: Iteractions.Iteractions[T], U: Factors.Factors[T], V: Factors.Factors[T]): Boolean = {
    val actualObjective = calcAll(iteractions, U, V)
    val result = actualObjective >= biggerObjective - epsilon
    if (actualObjective > biggerObjective) {
      biggerObjective = actualObjective
    }
    lastObjective = actualObjective
    result
  }

  protected[xclimf] def calcAll(iteractions: Iteractions.Iteractions[T],
    U: Factors.Factors[T], V: Factors.Factors[T]): Double = {
    val errors = iteractions.map {
      case (user, iteraction) => calcOne(iteraction)
    }.sum()
    (errors + regularization(U, V)) / U.count()
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
    val fmj_fmi = fmj.-:-(fmi)

    val b1 = log(sigmoid(fmiv))
    val rmjGfs = tile(rmj, 1, N).*:*(sigmoid(fmj_fmi))
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
}
