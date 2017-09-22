package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import scala.reflect.ClassTag
import com.timotta.spark.Blockfyier

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

  def prepare[T: ClassTag](ratingsByItems: RDD[(T, (T, Double))],
    numPartitions: Int,
    userFactors: Factors.Factors[T],
    itemFactors: Factors.Factors[T]): Iteractions[T] = {

    ratingsByItems.join(itemFactors, numPartitions).map {
      case (item, ((user, rating), itemFactor)) => (user, (item, rating, itemFactor))
    }.aggregateByKey((List.empty[T], Array[Double](), Array[Array[Double]]()), numPartitions)(
      { case (a, b) => (a._1 ++ List(b._1), a._2 ++ Array(b._2), a._3 ++ Array(b._3.toArray)) },
      { case (a, b) => (a._1 ++ b._1, a._2 ++ b._2, a._3 ++ b._3) }
    ).join(userFactors, numPartitions).map {
        case (user, ((itemNames, ratings, itemFactorsArrays), userFactors)) =>
          val itemRatings = DenseMatrix(ratings: _*).t
          val itemFactors = DenseMatrix(itemFactorsArrays: _*)
          (user, Iteractions.Iteraction(userFactors, itemNames, itemRatings, itemFactors))
    }
  }

  def prepare[T: ClassTag](ratingsByItems: RDD[(T, (T, Double))], numPartitions: Int, model: XCLiMFModel[T]): Iteractions[T] = {
    prepare(ratingsByItems, numPartitions, model.getUserFactors(), model.getItemFactors())
  }
}
