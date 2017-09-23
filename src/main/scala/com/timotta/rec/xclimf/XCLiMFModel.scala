package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import breeze.linalg.DenseMatrix
import com.timotta.spark.BoundedPriorityQueue
import com.timotta.spark.Blockfyier._

object XCLiMFModel {
  def apply[T: ClassTag](users: RDD[(T, Array[(T, Double)])], dims: Int) = {
    val userFactors = Factors.startUserFactors(users, dims)
    val itemFactors = Factors.startItemFactors(users, dims)
    new XCLiMFModel(userFactors, itemFactors)
  }
}

class XCLiMFModel[T: ClassTag](user: Factors.Factors[T], item: Factors.Factors[T]) extends Serializable {
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
    val toRecommend = if (itemFactors.count() < blockSize) {
      recommendByBroadcast(topK, ignoringByUser)
    } else {
      recommendByCartesian(topK, ignoringByUser, blockSize)
    }

    toRecommend.topByKey(topK)(Ordering.by(_._2))
  }

  private def recommendByBroadcast(topK: Int, ignoringByUser: RDD[(T, Set[T])]): RDD[(T, (T, Double))] = {
    val users = userFactors.leftOuterJoin(ignoringByUser, userFactors.partitions.size)
    val broadcast = itemFactors.sparkContext.broadcast(itemFactors.collect())
    val toRecommend = users.flatMap {
      case (user, (userFactors, ignore)) =>
        val items = broadcast.value
        recommendOneUser(topK, user, userFactors, items, ignore)
    }
    broadcast.unpersist()
    toRecommend
  }

  private def recommendByCartesian(topK: Int, ignoringByUser: RDD[(T, Set[T])], blockSize: Int = 4096): RDD[(T, (T, Double))] = {
    val usersBlocks = blockify(userFactors.leftOuterJoin(ignoringByUser, userFactors.partitions.size), blockSize)
    val itemsBlocks = blockify(itemFactors, blockSize)
    val cartesian = usersBlocks.cartesian(itemsBlocks)

    cartesian.flatMap {
      case (users, items) =>
        users.flatMap {
          case (user, (userFactors, ignore)) =>
            recommendOneUser(topK, user, userFactors, items, ignore)
        }
    }
  }

  private def recommendOneUser(topK: Int, user: T, userFactors: DenseMatrix[Double],
    items: Iterable[(T, DenseMatrix[Double])], ignore: Option[Set[T]]) = {
    val ranking = new BoundedPriorityQueue[(T, (T, Double))](topK)(Ordering.by(_._2._2))
    items.filter {
      case (item, _) => ignore match {
        case Some(ig) => !ig.contains(item)
        case None => true
      }
    }.foreach {
      case (item, itemFactors) =>
        val score = userFactors.toDenseVector.dot(itemFactors.toDenseVector)
        val r = (user, (item, score))
        ranking += r
    }
    ranking
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

}
