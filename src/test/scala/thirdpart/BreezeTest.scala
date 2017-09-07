package thirdpart

import breeze.linalg.tile
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import org.junit.Test
import org.junit.Assert

class BreezeTest {
  @Test
  def reshapeAndTile(): Unit = {
    val dense = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.015),
        DenseVector(0.05, 0.07, 0.11, 0.101),
        DenseVector(0.02, 0.01, 0.01, 0.03))

    val tiled = tile(dense, 1, 3)
    val expectedTiled = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.015, 0.01, 0.02, 0.03, 0.015, 0.01, 0.02, 0.03, 0.015),
        DenseVector(0.05, 0.07, 0.11, 0.101, 0.05, 0.07, 0.11, 0.101, 0.05, 0.07, 0.11, 0.101),
        DenseVector(0.02, 0.01, 0.01, 0.03, 0.02, 0.01, 0.01, 0.03, 0.02, 0.01, 0.01, 0.03))

    Assert.assertEquals(expectedTiled, tiled)

    val tiledReshaped = expectedTiled.reshape(9, 4)
    val expectedReshaped = DenseMatrix(
        DenseVector(0.01, 0.02, 0.03, 0.015),
        DenseVector(0.01, 0.02, 0.03, 0.015),
        DenseVector(0.01, 0.02, 0.03, 0.015),
        DenseVector(0.05, 0.07, 0.11, 0.101),
        DenseVector(0.05, 0.07, 0.11, 0.101),
        DenseVector(0.05, 0.07, 0.11, 0.101),
        DenseVector(0.02, 0.01, 0.01, 0.03),
        DenseVector(0.02, 0.01, 0.01, 0.03),
        DenseVector(0.02, 0.01, 0.01, 0.03))

    Assert.assertEquals(expectedTiled, tiledReshaped)
  }

  @Test
  def reshape(): Unit = {
    val dense = DenseMatrix(
        DenseVector(1D, 2D, 1D, 2D, 1D, 2D),
        DenseVector(3D, 4D, 3D, 4D, 3D, 4D))

    val reshaped = dense.reshape(6, 2, true)
    val expected = DenseMatrix(
        DenseVector(1D, 2D),
        DenseVector(1D, 2D),
        DenseVector(1D, 2D),
        DenseVector(3D, 4D),
        DenseVector(3D, 4D),
        DenseVector(3D, 4D))

    Assert.assertEquals(expected, reshaped)
  }

  @Test
  def reshape2(): Unit = {
    val dense = DenseMatrix(
        DenseVector(1D, 2D, 1D, 2D, 1D, 2D),
        DenseVector(3D, 4D, 3D, 4D, 3D, 4D))

    val reshaped = dense.reshape(1, 12, true)
    println(reshaped)

    val reshaped2 = reshaped.reshape(6, 2)
    println(reshaped2)

    val dense2 = DenseMatrix(
        DenseVector(1D, 2D),
        DenseVector(3D, 4D))

    println("-----")

    println(tile(dense2.t, 3, 1).reshape(2, 6).t)

    //val tiled = tile(dense2.reshape(4, 1), 1, 3)

    //println(tiled)
  }

}
