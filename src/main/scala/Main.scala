import com.timotta.rec.xclimf.XCLiMF
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix
import com.timotta.rec.xclimf.Iteractions

object Main extends App {

  val x = new XCLiMF[String](dims=5)

  val user = DenseVector.rand(5)
  val ite1 = DenseVector.rand(5)
  val ite2 = DenseVector.rand(5)

  println("user", user)
  println("ite1", ite1)
  println("ite2", ite2)

  val iteraction = Iteractions.Iteraction(user, List("i1", "i2"), DenseVector(0.1f, 0.2f), DenseMatrix(ite1, ite2))

  x.updateOneUser("u1", iteraction)

}
