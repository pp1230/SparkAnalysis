import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by pi on 17-12-13.
  */
object SparkConfiguration {
//
//  val conf = new SparkConf().setAppName("History Analysis").setMaster("local[*]")
//  val ss = SparkSession.builder().config(conf).getOrCreate()

    var conf = new SparkConf().setAppName("cluster").setMaster("spark://172.31.34.183:7077")
        .set("spark.submit.deployMode","client")
        //.set("spark.memory.fraction","0.4")
                      .set("spark.driver.maxResultSize","12g")
                    .set("spark.driver.cores","8")
                    .set("spark.driver.memory","8g")
                    .set("spark.executor.memory","16g")
                    .set("spark.executor.cores","4")
    val ss = SparkSession.builder().config(conf).config("spark.jars","target/SparkAnalysis.jar").getOrCreate()

  def main(args: Array[String]) {
    print("Hello Spark!")
  }
}
