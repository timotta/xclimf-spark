package com.timotta.rec.xclimf

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

case class Rating[T](user: T, item: T, rating: Double)

object Rating {
  def prepare[T: ClassTag](ratings: RDD[Rating[T]], ignoreTopK: Int): RDD[Rating[T]] = {
    normalize(ignoreGlobalTopK(ratings, ignoreTopK))
  }

  private def ignoreGlobalTopK[T: ClassTag](ratings: RDD[Rating[T]], ignoreTopK: Int): RDD[Rating[T]] = {
    val toIgnore = ratings.map { r => (r.item, r.rating) }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .map(_._1)
      .take(ignoreTopK)
      .toList
    ratings.filter(r => !toIgnore.contains(r.item))
  }

  private def normalize[T](ratings: RDD[Rating[T]]): RDD[Rating[T]] = {
    val min = ratings.min()(Ordering.by(_.rating)).rating
    val max = ratings.max()(Ordering.by(_.rating)).rating
    if (min == 0) {
      val rmax = max + 1
      ratings.map { r => r.copy(rating = (r.rating + 1.0) / rmax) }
    } else if (min < 0) {
      val rmin = math.abs(min) + 1
      val rmax = max + rmin
      ratings.map { r => r.copy(rating = (r.rating + rmin) / rmax) }
    } else {
      ratings.map { r => r.copy(rating = r.rating / max) }
    }
  }

  def topKByUser[T: ClassTag](ratings: RDD[Rating[T]], topK: Int) = {
    ratings.map(r => (r.user, (r.item, r.rating))).topByKey(topK)(Ordering.by(_._2))
  }
}
