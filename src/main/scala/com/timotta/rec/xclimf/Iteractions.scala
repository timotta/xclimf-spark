package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import scala.reflect.ClassTag

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

  def prepare[T: ClassTag](users: RDD[(T, Array[(T, Double)])],
    userFactors: Factors.Factors[T],
    itemFactors: Factors.Factors[T]): Iteractions[T] = {
    val numPartitions = users.partitions.size
    users.flatMap {
      case (user, ratings) =>
        ratings.map {
          case (item, rating) =>
            (item, (user, rating))
        }
    }.join(itemFactors).map {
      case (item, ((user, rating), itemFactor)) =>
        (user, (item, rating, itemFactor))
    }.groupBy(_._1).join(userFactors).map {
      case (user, (items, userFactors)) =>
        val itemNames = items.map(_._2._1).toList
        val itemRatings = DenseMatrix(items.map(_._2._2).toList: _*).t
        val itemFactors = DenseMatrix(items.map(_._2._3.toArray).toArray: _*)
        (user, Iteractions.Iteraction(userFactors, itemNames, itemRatings, itemFactors))
    }.repartition(numPartitions)
  }

  def prepare[T: ClassTag](users: RDD[(T, Array[(T, Double)])], model: XCLiMFModel[T]): Iteractions[T] = {
    prepare(users, model.getUserFactors(), model.getItemFactors())
  }
}
