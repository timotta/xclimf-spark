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
}
