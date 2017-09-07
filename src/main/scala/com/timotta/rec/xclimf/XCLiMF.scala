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
      val updated = updateOneUser(user, iteraction)
      (user, updated)
    }
  }

  def updateOneUser(user: T, iteraction: Iteractions.Iteraction[T]) = {
    val N = iteraction.itemNames.size
    val fmiv = iteraction.itemFactors.*(iteraction.userFactors).toDenseMatrix
    val fmi = tile(fmiv, N, 1)
    val fmk = fmi.t

    val fmi_fmk = fmi.-:-(fmk)
    val fmk_fmi = fmk.-:-(fmi)
    val ymi = iteraction.itemRatings.toDenseMatrix
    val ymk = ymi.t
    val ymitile = tile(ymi, fmi_fmk.rows, 1)
    val ymktile = tile(ymk, 1, fmk_fmi.cols)
    val g_fmi = sigmoid(-1.0 * fmiv)

    //items partial increments
    val div1 = sigdivision(ymktile, fmk_fmi)
    val div2 = sigdivision(ymitile, fmi_fmk)
    val bimul = ymktile.*:*(dg(fmi_fmk).*:*(div1 - div2))
    val brackets_i = g_fmi + sum(bimul(::, *))
    val ymibru = ymi.*:*(brackets_i).t.*(iteraction.userFactors.toDenseMatrix)
    val di = gamma * (ymibru - lambda * iteraction.itemFactors)

    val top = ymktile.*:*(dg(fmk_fmi))
    val bot = invsig(ymktile, fmk_fmi)
    val N2 = N * N
    val sub = factorsubtract(N, N2, iteraction.itemFactors)
    val top_bot = top./:/(bot).reshape(N2, 1)
    val top_bot_sub = tile(top_bot, 1, sub.cols).*:*(sub)
    val brackets_uk = tensor3dsum(N, top_bot_sub)
    val brackets_ui = tile(g_fmi.t, 1, dims).*:*(iteraction.itemFactors)

    println("\n", brackets_uk)
    println("\n", brackets_ui)

    val brackets_u = brackets_ui + brackets_uk

    println("\n", brackets_u)

    Iteractions.Iteraction[T](iteraction.userFactors, iteraction.itemNames, iteraction.itemRatings, di)
  }

  private def factorsubtract(N:Int, N2:Int, factors: DenseMatrix[Double]): DenseMatrix[Double] = {
    //Correct code if Breeze reshape was working properly:
    //  in breeze: val vis = tile(iteraction.itemFactors, 1, N).reshape(N2, dims)
    //In numpy works:
    //  in numpy:  np.tile(viks, (1, N)).reshape(N2, D)
    val vis = tile(factors.t, N, 1).reshape(dims, N2).t
    val vks = tile(factors, N, 1)
    vis.-(vks)
  }

  private def sigdivision(ymx: DenseMatrix[Double], fmx_fmy: DenseMatrix[Double]): DenseMatrix[Double] = {
    1.0 / invsig(ymx, fmx_fmy)
  }

  private def invsig(ymx: DenseMatrix[Double], fmx_fmy: DenseMatrix[Double]): DenseMatrix[Double] = {
    1.0 - ( ymx.*:*( sigmoid(fmx_fmy) ) )
  }

  private def tensor3dsum(N: Int, matrix: DenseMatrix[Double]):DenseMatrix[Double] = {
    //Doing loop here because breeze doesnt have 3d tensors like numpy
    // this in numpy: np.sum((top_bot * sub).reshape(N, N, D), axis=1)
    val r = 0.to(N-1).map { i =>
      val start = i * N
      val end = start + N
      val sl = start.until(end)
      val n = matrix(sl, ::)
      sum(n, Axis._0).t
    }
    DenseMatrix(r.toArray:_*)
  }

  private def dg(x:DenseMatrix[Double]): DenseMatrix[Double] = {
    exp(x)/pow((exp(x) + 1.0), 2)
  }
}
