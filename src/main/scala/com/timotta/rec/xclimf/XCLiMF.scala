package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.sql.Dataset
import scala.reflect.ClassTag
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import scala.math.Ordering
import breeze.linalg._
import breeze.numerics._

/**
 * @maxIters: Max number of iteractions to optimize
 * @dims: Number of latent factors features
 * @gamma: Step size of each iteraction
 * @lambda: Regularization factor for avoiding bias
 * @topK: Consider only topk itens for each user
 * @ignoreTopK: Number of global topK items to ignore
 * @epsilon: Error tolerance for stopying gradient
 */
class XCLiMF[T: ClassTag](
  maxIters: Int = 25,
  dims: Int = 10,
  gamma: Double = 0.001f,
  lambda: Double = 0.001f,
  topK: Int = 5,
  ignoreTopK: Int = 3,
  epsilon: Double = 1e-4f) extends Serializable {

  def fit(ratings: RDD[Rating[T]]) {
    val validRatings = ignoreGlobalTopK(ratings)
    val users = selectTopKByUser(validRatings)
    var userFactors = Factors.startUserFactors(users, dims)
    var itemFactors = Factors.startItemFactors(validRatings, dims)

    0.to(maxIters).foreach { i =>
      val iteraction = prepareIteraction(users, userFactors, itemFactors)
      val updated = update(iteraction)

      itemFactors = Factors.asItemFactors(updated, itemFactors)
      userFactors = Factors.asUserFactors(updated)
    }
  }

  private def ignoreGlobalTopK(ratings: RDD[Rating[T]]) = {
    val toIgnore = ratings.map { r => (r.item, r.rating) }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .map(_._1)
      .take(ignoreTopK)
      .toList
    ratings.filter(r => !toIgnore.contains(r.item))
  }

  private def selectTopKByUser(ratings: RDD[Rating[T]]) = {
    ratings.map(r => (r.user, (r.item, r.rating))).topByKey(topK)(Ordering.by(_._2))
  }

  private def prepareIteraction(users: RDD[(T, Array[(T, Float)])],
    userFactors: RDD[(T, DenseVector[Double])],
    itemFactors: RDD[(T, DenseVector[Double])]): Iteractions.Iteractions[T] = {
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
        val itemRatings = DenseVector(items.map(_._2._2.toDouble).toList:_*)
        val itemFactors = DenseMatrix(items.map(_._2._3).toList:_*)
        (user, Iteractions.Iteraction(userFactors, itemNames, itemRatings, itemFactors))
    }
  }

  private def update(iteractions: Iteractions.Iteractions[T]) = {
    iteractions.map { case (user, iteraction) =>
      updateOneUser(user, iteraction)
    }
    iteractions
  }

  def updateOneUser(user: T, iteraction: Iteractions.Iteraction[T]) = {
    val N = iteraction.itemNames.size
    val fmi = tile(iteraction.itemFactors.*(iteraction.userFactors).toDenseMatrix, N, 1)
    val fmk = fmi.t

    val fmi_fmk = fmi.-:-(fmk)
    val fmk_fmi = fmk.-:-(fmi)

    println("fmi_fmk",  fmi_fmk)
    println("fmk_fmi",  fmk_fmi)
  }
}
