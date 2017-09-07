package thirdpart

import org.junit.Test
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.linalg.Matrices

class MLLibMatrixTest {

  @Test
  def basic() {

    val dense = Matrices.dense(2, 4, Array(1D, 3D, 2D, 4D, 1D, 3D, 2D, 4D))

    println(dense)

    println(dense.toArray.toList)

    println(dense.toArray(0), dense.toArray(2))
    println(dense.toArray(4), dense.toArray(6))
    println(dense.toArray(1), dense.toArray(3))
    println(dense.toArray(5), dense.toArray(7))

    println("....")

    val dense2 = Matrices.dense(4, 2, dense.toArray)

    println(dense2)

  }

}
