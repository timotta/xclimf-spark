package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector

object Iteractions {
  case class Iteraction[T](
    userFactors: DenseVector[Double],
    itemNames: List[T],
    itemRatings: DenseVector[Double],
    itemFactors: DenseMatrix[Double])

  type Iteractions[T] = RDD[(T, Iteraction[T])]
}
