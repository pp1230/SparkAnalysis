import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrix, SparseMatrix}
//import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object SVDTest {
  val session = SparkConfiguration.ss
  val sc = session.sparkContext

  def main(args: Array[String]): Unit = {
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))

    val rows = sc.parallelize(data)

    val mat: RowMatrix = new RowMatrix(rows)

    // Compute the top 5 singular values and corresponding singular vectors.
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(5, computeU = true)
    val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
    val s: Vector = svd.s     // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V     // The V factor is a local dense matrix.

    import session.implicits._
    val features = new SparseMatrix(5, 5, Array.range(0,6),Array.range(0,5),s.toArray).toDense
    features.values.foreach(x => print(x+" "))
    val mul = U.multiply(features).multiply(V.transpose).rows
      .map(v => v.toArray.map(x => {
      if(x < 0.1) 0
      else x
    })).toDF()
    mul.show(false)

    val result = mul.select($"value", posexplode($"value")).toDF("value", "item_id", "s")
    result.show(false)
//    val idRow = mul.withColumn("", posexplode(mul.col("value")))
//    idRow.show(false)
  }

}
