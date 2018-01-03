
import org.apache.spark.mllib.linalg.{SparseMatrix, Vector}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.functions._

object LRTest {
  val session = SparkConfiguration.ss
  case class Stucture(id:Long, value:Array[Double])

//  val rootPath = "/home/pi/doc/dataset/output/"
//  val dataPath = "YelpTextFeatureLDA30Rev20JoinEm0.8Num"
//  val savePath = "rematrix"

  val rootPath = "hdfs://172.31.34.183:9000/"
  val dataPath = "outputdata/YelpTextFeatureLDA30JoinEmRev20Num"
  val savePath = "outputdata/ReMatrix"
  val trainingPath = "outputdata/Training0.8"
  val testingPath = "outputdata/Testing0.8"
  def main(args: Array[String]): Unit = {
//    val output1 = session.read.parquet(rootPath+dataPath)
//    val Array(training, testing) = output1.randomSplit(Array(8,2))
//    output1.show(false)
//    training.write.json(rootPath+trainingPath)
//    testing.write.json(rootPath+testingPath)

    val training = session.read.json(rootPath+trainingPath)
    val output2 = training.select("user_id", "business_id", "s").groupBy("user_id", "business_id").avg("s")
      .orderBy("user_id", "business_id")
    //output2.show(false)
    val entries = output2.collect()
      .map(x => new MatrixEntry(x.getLong(0), x.getLong(1), x.getDouble(2)))
    //entries.foreach(println(_))
    val rddEntries = session.sparkContext.parallelize(entries)
    val coordinateMatrix = new CoordinateMatrix(rddEntries)
    val rowMatrix = coordinateMatrix.toIndexedRowMatrix()
    val k = 10
    val svd = rowMatrix.computeSVD(k, true)
    //println(svd.s)
    import session.implicits._
    val features = new SparseMatrix(k, k, Array.range(0,k+1),Array.range(0,k),svd.s.toArray).toDense
    val mul = svd.U.multiply(features).multiply(svd.V.transpose).rows.toDF().orderBy("index")
    //mul.show()

    val indexAndArr = mul.map(x => Stucture(x.getLong(0), x.getAs[Vector](1).toArray))
    //indexAndArr.show()
    val result = indexAndArr.select($"id", $"value", posexplode($"value")).toDF("user_id", "vector","item_id", "s")
    //result.show()

    val filtered = result.filter(result.col("s")>0.1)
    //filtered.show()
    //println("Number:"+filtered.count())
    filtered.write.json(rootPath+savePath)

  }
}
