package com.timotta.rec.xclimf

import org.apache.spark.api.java.JavaRDD
import java.util.Map
import org.apache.spark.rdd.RDD
import breeze.linalg.DenseMatrix

class PyXCLiMF(maxIters: Int = 25,
  dims: Int = 10,
  gamma: Double = 0.001f,
  lambda: Double = 0.001f,
  topK: Int = 5,
  ignoreTopK: Int = 3,
  epsilon: Double = 1e-4f) extends XCLiMF[String](maxIters, dims, gamma, lambda, topK, ignoreTopK, epsilon) {

  def fit(jrdd: JavaRDD[Map[String, Object]]): PyXCLiMFModel = {
    val rdd = jrdd.rdd.map { m =>
      (m.get("user"), m.get("item"), m.get("rating")) match {
        case v: (String, String, Double) => Some(Rating[String](v._1, v._2, v._3))
        case _ => None
      }
    }.collect { case Some(s) => s }
    new PyXCLiMFModel(fit(rdd))
  }
}

class PyXCLiMFModel(model: XCLiMFModel[String]) {
  def getUserFactors(): RDD[Array[Any]] = {
    toPython(model.getUserFactors())
  }

  def getItemFactors(): RDD[Array[Any]] = {
    toPython(model.getItemFactors())
  }

  def toPython(rdd: RDD[(String, DenseMatrix[Double])]): RDD[Array[Any]] = {
    rdd.map { case (n, v) => Array(n, v.toArray) }.asInstanceOf[RDD[Array[Any]]]
  }
}
