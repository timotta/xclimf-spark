package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

class XCLiMFModel[T: ClassTag](users: RDD[(T, Array[(T, Double)])], dims: Int) {
  private var userFactors = Factors.startUserFactors(users, dims)
  private var itemFactors = Factors.startItemFactors(users, dims)

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
}
