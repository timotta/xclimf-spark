package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import scala.reflect.ClassTag
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._

object Factors {

  type Factors[T] = RDD[(T, DenseMatrix[Double])]

  def startUserFactors[T](users: RDD[(T, Array[(T, Double)])], dims: Int): Factors[T] = {
    users.map { case (user, _) => (user, DenseMatrix.rand(1, dims) * 0.01) }
  }

  def startItemFactors[T: ClassTag](users: RDD[(T, Array[(T, Double)])], dims: Int): Factors[T] = {
    users.flatMap { case (_, items) => items.map(_._1) }.distinct()
      .map { item => (item, DenseMatrix.rand(1, dims) * 0.01) }
  }

  def asItemFactors[T: ClassTag](iteractions: Iteractions.Iteractions[T], itemFactors: Factors[T]): Factors[T] = {
    iteractions.flatMap {
      case (_, Iteractions.Iteraction(_, itemNames, itemRatings, itemFactors)) =>
        0.to(itemNames.size - 1).map { i =>
          (itemNames(i), itemFactors(i, ::).t.toDenseMatrix)
        }
    }.++(itemFactors).reduceByKey(_ + _)
  }

  def asUserFactors[T](iteraction: Iteractions.Iteractions[T]) = {
    iteraction.map { case (user, Iteractions.Iteraction(factors, _, _, _)) => (user, factors) }
  }
}
