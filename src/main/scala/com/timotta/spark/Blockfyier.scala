package com.timotta.spark

import org.apache.spark.rdd.RDD

object Blockfyier {
  def blockify[T, V](features: RDD[(T, V)], blockSize: Int) = {
    features.mapPartitions { iter =>
      iter.grouped(blockSize)
    }
  }
}
