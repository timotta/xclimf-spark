package com.timotta.rec.xclimf
import breeze.linalg._
import breeze.numerics._

/**
 * functions to isolate IDE problems on scalar/matrix operations
 */
object ScalarMatrixOps {
  def mul(v: Double, m: DenseMatrix[Double]): DenseMatrix[Double] = {
    v * m
  }
  def add(v: Double, m: DenseMatrix[Double]): DenseMatrix[Double] = {
    m + v
  }
  def dif(v: Double, m: DenseMatrix[Double]): DenseMatrix[Double] = {
    v - m
  }
  def div(v: Double, m: DenseMatrix[Double]): DenseMatrix[Double] = {
    v / m
  }
}
