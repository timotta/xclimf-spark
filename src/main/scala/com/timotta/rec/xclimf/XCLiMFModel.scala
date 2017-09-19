package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import breeze.linalg.DenseMatrix
import com.timotta.spark.BoundedPriorityQueue

object XCLiMFModel {
  def apply[T: ClassTag](users: RDD[(T, Array[(T, Double)])], dims: Int) = {
    val userFactors = Factors.startUserFactors(users, dims)
    val itemFactors = Factors.startItemFactors(users, dims)
    new XCLiMFModel(userFactors, itemFactors)
  }
}

class XCLiMFModel[T: ClassTag](user: Factors.Factors[T], item: Factors.Factors[T]) {
  private var userFactors = user
  private var itemFactors = item

  def update(updated: RDD[(T, Iteractions.Iteraction[T])]) {
    val newI = Factors.asItemFactors(updated, itemFactors)
    itemFactors.unpersist()
    itemFactors = newI
    val newU = Factors.asUserFactors(updated)
    userFactors.unpersist()
    userFactors = newU
  }

  def getUserFactors() = {
    userFactors
  }

  def getItemFactors() = {
    itemFactors
  }

  /**
   * @topK: number of items to return
   * @ignoringByUser: Tuple RDD of items to not recommend for each user _1 = user, _2 = items
   * @blockSize: Performance parameter to represent number of user and items per partitions
   */
  def recommend(topK: Int, ignoringByUser: RDD[(T, Set[T])], blockSize: Int = 4096): RDD[(T, Array[(T, Double)])] = {
    val usersFactors = blockify(getUserFactors().leftOuterJoin(ignoringByUser), blockSize)
    val itemsFactors = blockify(getItemFactors(), blockSize)
    usersFactors.cartesian(itemsFactors).flatMap {
      case (users, items) =>
        users.flatMap {
          case (user, (userFactors, ignore)) =>
            val ranking = new BoundedPriorityQueue[(T, Double)](topK)(Ordering.by(_._2))
            items.filter {
              case (item, _) => ignore match {
                case Some(ig) => !ig.contains(item)
                case None => true
              }
            }.foreach {
              case (item, itemFactors) =>
                val r = (item, userFactors.toDenseVector.dot(itemFactors.toDenseVector))
                ranking += r
            }
            ranking.map {
              case (i, v) =>
                (user, (i, v))
            }
        }
    }.topByKey(topK)(Ordering.by(_._2))
  }

  /**
   * @topK: number of items to return
   */
  def recommend(topK: Int): RDD[(T, Array[(T, Double)])] = {
    val empty = getUserFactors().sparkContext.emptyRDD[(T, Set[T])]
    recommend(topK, empty)
  }

  /**
   * @topK: number of items to return
   * @blockSize: Performance parameter to represent number of user and items per partitions
   */
  def recommend(topK: Int, blockSize: Int): RDD[(T, Array[(T, Double)])] = {
    val empty = getUserFactors().sparkContext.emptyRDD[(T, Set[T])]
    recommend(topK, empty, blockSize)
  }

  private def blockify[V](features: RDD[(T, V)], blockSize: Int) = {
    features.mapPartitions { iter =>
      iter.grouped(blockSize)
    }
  }
}
