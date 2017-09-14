package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector

object Iteractions {
  case class Iteraction[T](
    userFactors: DenseMatrix[Double],
    itemNames: List[T],
    itemRatings: DenseMatrix[Double],
    itemFactors: DenseMatrix[Double]) {

    def fmi(): DenseMatrix[Double] = {
      userFactors.*(itemFactors.t)
    }
  }

  type Iteractions[T] = RDD[(T, Iteraction[T])]
}
