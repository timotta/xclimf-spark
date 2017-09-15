package com.timotta.rec.xclimf

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import scala.math.Ordering
import breeze.linalg._
import breeze.numerics._
import breeze.math._
import ScalarMatrixOps._
import com.timotta.rec.xclimf.Iteractions.Iteraction
import scala.util.control.Breaks._
import org.apache.log4j.LogManager

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

  @transient private val log = LogManager.getLogger(getClass)

  def fit(ratings: RDD[Rating[T]]): XCLiMFModel[T] = {
    val validRatings = Rating.prepare(ratings, ignoreTopK)
    val users = Rating.topKByUser(validRatings, topK)

    val model = new XCLiMFModel[T](users, dims)

    val max = validRatings.max()(Ordering.by(_.rating)).rating
    val objective = new ObjectiveTrack[T](max, lambda, epsilon)

    breakable {
      for (i <- 1 to maxIters) {
        val iteraction = Iteractions.prepare(users, model)
        if (objective.update(iteraction, model)) {
          log.info("doing iteraction=" + i + ": last objective=" + objective.lastObjective)
          val updated = update(iteraction)
          model.update(updated)
        } else {
          log.info("stopying at iteraction=" + i + ": last objective=" + objective.lastObjective)
        }
      }
    }

    model
  }

  private def update(iteractions: Iteractions.Iteractions[T]) = {
    iteractions.map {
      case (user, iteraction) =>
        val updated = updateOneUser(user, iteraction)
        (user, updated)
    }
  }

  protected[xclimf] def updateOneUser(user: T, iteraction: Iteraction[T]): Iteractions.Iteraction[T] = {
    val N = iteraction.itemNames.size

    val fmiv = fmiVector(iteraction)

    val fmi = tile(fmiv, N, 1)
    val fmk = fmi.t
    val fmi_fmk = fmi.:-(fmk)
    val fmk_fmi = fmk.:-(fmi)

    val ymi = iteraction.itemRatings
    val ymk = ymi.t
    val ymitile = tile(ymi, fmi_fmk.rows, 1)
    val ymktile = tile(ymk, 1, fmk_fmi.cols)
    val g_fmi = sigmoid(mul(-1.0, fmiv))

    //items partial increments (will sum after all users looping)
    val div1 = sigdivision(ymktile, fmk_fmi)
    val div2 = sigdivision(ymitile, fmi_fmk)
    val bimul = ymktile.:*(dg(fmi_fmk).:*(div1 - div2))
    val brackets_i = g_fmi + sum(bimul(::, *))
    val ymibru = ymi.:*(brackets_i).t.*(iteraction.userFactors)
    val di = mul(gamma, (ymibru - mul(lambda, iteraction.itemFactors)))

    //user partial increment
    val top = ymktile.:*(dg(fmk_fmi))
    val bot = invsig(ymktile, fmk_fmi)
    val N2 = N * N
    val sub = factorsubtract(N, N2, iteraction.itemFactors)
    val top_bot = top.:/(bot).reshape(N2, 1)
    val top_bot_sub = tile(top_bot, 1, sub.cols).:*(sub)
    val brackets_uk = tensor3dsum(N, top_bot_sub)
    val brackets_ui = tile(g_fmi.t, 1, dims).:*(iteraction.itemFactors)
    val brackets_u = brackets_ui + brackets_uk
    val du1 = iteraction.itemRatings * brackets_u
    val bias = mul(lambda, iteraction.userFactors)
    val du2 = mul(gamma, du1 - bias)

    //user factor summed
    val userFactors = iteraction.userFactors + du2

    Iteraction[T](userFactors, iteraction.itemNames, iteraction.itemRatings, di)
  }

  private def fmiVector(iteraction: Iteraction[T]): DenseMatrix[Double] = {
    iteraction.userFactors.*(iteraction.itemFactors.t)
  }

  private def factorsubtract(N: Int, N2: Int, factors: DenseMatrix[Double]): DenseMatrix[Double] = {
    //Correct code if Breeze was row orientend:
    //  in breeze: val vis = tile(factors, 1, N).reshape(N2, dims)
    //In numpy works:
    //  in numpy:  np.tile(viks, (1, N)).reshape(N2, D)
    val vis = tile(factors.t, N, 1).reshape(dims, N2).t
    val vks = tile(factors, N, 1)
    vis.-(vks)
  }

  private def sigdivision(ymx: DenseMatrix[Double], fmx_fmy: DenseMatrix[Double]): DenseMatrix[Double] = {
    div(1.0, invsig(ymx, fmx_fmy))
  }

  private def invsig(ymx: DenseMatrix[Double], fmx_fmy: DenseMatrix[Double]): DenseMatrix[Double] = {
    dif(1.0, (ymx.:*(sigmoid(fmx_fmy))))
  }

  private def tensor3dsum(N: Int, matrix: DenseMatrix[Double]): DenseMatrix[Double] = {
    //Doing loop here because breeze doesnt have 3d tensors like numpy
    // this in numpy: np.sum((top_bot * sub).reshape(N, N, D), axis=1)
    val r = 0.to(N - 1).map { i =>
      val start = i * N
      val end = start + N
      val sl = start.until(end)
      val n = matrix(sl, ::)
      sum(n, Axis._0).t
    }
    DenseMatrix(r.toArray: _*)
  }

  private def dg(x: DenseMatrix[Double]): DenseMatrix[Double] = {
    exp(x) / pow(add(1.0, exp(x)), 2)
  }

}
