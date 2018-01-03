import PrivacyTest.{rootPath, session}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.Row

/**
  * Created by pi on 17-12-27.
  */

object PrivacyTest {
  val session = SparkConfiguration.ss
  val rootPath = "/Users/chenyang/Downloads/"
  val dataPath = "YelpTextFeatureLDA30Rev20JoinEm0.7NumItem"
  def main(args: Array[String]) {
    val output1 = session.read.parquet(rootPath+dataPath)
    output1.show(false)
    println("Total Number:"+output1.count())

    val Array(training,testing) = output1.randomSplit(Array(8,2))
//    training.write.parquet(rootPath+"training")
//    testing.write.parquet(rootPath+"testing")

//    val training = session.read.parquet(rootPath+"training")
//    val testing = session.read.parquet(rootPath+"testing")

    val LR = new LinearRegression().setFeaturesCol("topicDistribution")
      .setLabelCol("s")
      .setPredictionCol("prediction")

    val fit = LR.fit(training)
    val result = fit.transform(testing)
    result.show()
    val evaluator = new RegressionEvaluator()
    val e = evaluator.setPredictionCol("prediction")
      .setLabelCol("s")
      .setMetricName("rmse")
    val rmse = e.evaluate(result)
    println("RMSE:"+rmse)
    println("Param:"+fit.coefficients+fit.intercept)
    fit.save("/Users/chenyang/Downloads/fit")

    val Array(data1, data2, data3) = training.randomSplit(Array(1, 1, 1))
    val model1 = LR.fit(data1)
    val model2 = LR.fit(data2)
    val model3 = LR.fit(data3)
    println("Param1:"+model1.coefficients+model1.intercept)
    println("Param2:"+model2.coefficients+model2.intercept)
    println("Param3:"+model3.coefficients+model3.intercept)
    val coefficient1 = model1.coefficients.toArray
    val coefficient2 = model2.coefficients.toArray
    val coefficient3 = model3.coefficients.toArray
    val len = coefficient1.size
    val coefficient = new Array[Double](len)
    for(i <- 0 to len-1){
      coefficient.update(i, (coefficient1.apply(i)+coefficient2.apply(i)+coefficient3.apply(i))/3.0)
    }
    val intercept = (model1.intercept+model2.intercept+model3.intercept)/3.0
    val coefficient4 = new DenseVector(coefficient)
    import session.implicits._
    val output2 = session.read.parquet("/Users/chenyang/Downloads/fit/data")
    output2.map(row => (intercept,coefficient4)).toDF("intercept","coefficients")
      .write.mode("overwrite").parquet("/Users/chenyang/Downloads/fit1/data")
    val model = LinearRegressionModel.load("/Users/chenyang/Downloads/fit1")
        val result4 = model.transform(testing)
        println("Param3:"+model.coefficients+model.intercept)
        result4.show()
        val evaluator3 = new RegressionEvaluator()
        val e4 = evaluator3.setPredictionCol("prediction")
          .setLabelCol("s")
          .setMetricName("rmse")
        val rmse4 = e4.evaluate(result4)
        println("RMSE:"+rmse4)
  }
}

object ShowData{
  val session = SparkConfiguration.ss
  val rootPath = "/Users/chenyang/Downloads/"
  val dataPath = "fit/data"
  def main(args: Array[String]): Unit = {
    import session.implicits._
    val output1 = session.read.parquet(rootPath+dataPath)
    output1.show(false)
    println("Total Number:"+output1.count())
    output1.map(row => (0,(0,0,0))).toDF("intercept","coefficients").write.parquet("/Users/chenyang/Downloads/test")
  }
}
