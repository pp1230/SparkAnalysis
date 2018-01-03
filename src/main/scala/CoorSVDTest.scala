
import org.apache.spark.mllib.linalg.{Vector, SparseMatrix, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.functions._

/**
  * Created by pi on 18-1-3.
  */
object CoorSVDTest {
  val session = SparkConfiguration.ss
  val sc = session.sparkContext
  case class Stucture(id:Long, value:Array[Double])

  val rootPath = "hdfs://172.31.34.183:9000/"
  val dataPath = "outputdata/YelpTextFeatureLDA30JoinEmRev20Num"
  val savePath = "outputdata/ReMatrix"
  def main(args: Array[String]) {
    val data = Array(
      new MatrixEntry(0, 0, 1),
      new MatrixEntry(0, 1, 0),
      new MatrixEntry(0, 2, 3),
      new MatrixEntry(1, 0, 2),
      new MatrixEntry(1, 1, 4),
      new MatrixEntry(2, 2, 3))

    val rows = sc.parallelize(data)
    val rddEntries = session.sparkContext.parallelize(data)
    val coordinateMatrix = new CoordinateMatrix(rddEntries)
    val rowMatrix = coordinateMatrix.toIndexedRowMatrix()
    val k = 2
    val svd = rowMatrix.computeSVD(k, true)
    println(svd.s)
    import session.implicits._
    val features = new SparseMatrix(k, k, Array.range(0,k+1),Array.range(0,k),svd.s.toArray).toDense
    val mul = svd.U.multiply(features).multiply(svd.V.transpose).rows.toDF().orderBy("index")
    mul.show(false)

    val indexAndArr = mul.map(x => Stucture(x.getLong(0), x.getAs[Vector](1).toArray))
    indexAndArr.show(false)
    val result = indexAndArr.select($"id", $"value", posexplode($"value")).toDF("user_id", "vector","item_id", "s")
    result.show(false)

    val filtered = result.filter(result.col("s")>0.1)
    println("Number:"+filtered.count())
    filtered.show(false)
    //filtered.write.json(rootPath+savePath)
  }
}
