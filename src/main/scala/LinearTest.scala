/**
  * Created by pi on 18-1-3.
  */
object LinearTest {
  val session = SparkConfiguration.ss
    val rootPath = "/home/pi/doc/dataset/output/"
    val dataPath = "YelpTextFeatureLDA30Rev20JoinEm0.8Num"
    val savePath = "rematrix"

//  val rootPath = "hdfs://172.31.34.183:9000/"
//  val dataPath = "outputdata/YelpTextFeatureLDA30JoinEmRev20Num"
//  val savePath = "outputdata/ReMatrix"
  def main(args: Array[String]) {
  val output1 = session.read.parquet(rootPath+dataPath)
  val Array(training, testing) = output1.randomSplit(Array(8,2))
  output1.show(false)
  val dataframe = LinearRegressionAl.fit(training, "topicDistribution", "s")
  val result = dataframe.transform(testing)
  result.show()
  val metric = LinearRegressionAl.evaluate(result)
  println(metric)
  }
}
