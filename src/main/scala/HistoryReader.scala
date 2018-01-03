
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType}

/**
  * Created by pi on 17-12-13.
  */

object HistoryReader {
  val session = SparkConfiguration.ss


  def main(args: Array[String]) {
    import session.implicits._
    val history = session.read.json("/home/pi/doc/history/records.csv")
      .orderBy("submitTime")
    //history.show()
    val time1 = history.withColumn("start", unix_timestamp(history.col("submitTime")))
    time1.show()
    val time2 = time1.select("duration", "jobId", "sql", "status", "tasks", "start")
    //time2.show()

    val time3 = time2.map(row => {
      val duration = row(0).toString
      if (duration.contains("s"))
        (Math.round(duration.split(" ")(0).toFloat), row(1).toString, row(2).toString,
          row(3).toString, row(4).toString, row(5).toString.toInt)
      else if (duration.contains("ms"))
        (0, row(1).toString, row(2).toString,
          row(3).toString, row(4).toString, row(5).toString.toInt)
      else if (duration.contains("min"))
        (Math.round(duration.split(" ")(0).toFloat * 60), row(1).toString, row(2).toString,
          row(3).toString, row(4).toString, row(5).toString.toInt)
      else if (duration.contains("h"))
        (Math.round(duration.split(" ")(0).toFloat * 60 * 60), row(1).toString, row(2).toString,
          row(3).toString, row(4).toString, row(5).toString.toInt)
      else
        (0, row(1).toString, row(2).toString,
          row(3).toString, row(4).toString, row(5).toString.toInt)
    }).toDF("duration", "jobId", "sql", "status", "tasks", "start")
    //time3.show()

    val time4 = time3.map(row => {
      val duration = row.getInt(0)
      val start = row.getInt(5)
      val end = start + duration
      (row(1).toString, row(2).toString,
        row(3).toString, row(4).toString, row(5).toString.toInt, end)
    }).toDF("jobId", "sql", "status", "tasks", "start", "end")
    //time4.show()

    val time5 = time4.map(row => {
      val start = row.getInt(4)
      val end = row.getInt(5)
      val array = Array.range(start, end)
      (row(0).toString, row(1).toString,
        row(2).toString, row(3).toString, row(4).toString.toInt, row(5).toString.toInt, array)
    }).toDF("jobId", "sql", "status", "tasks", "start", "end", "array")
    time5.show()

    val time6 = time5.withColumn("sec", explode(time5.col("array")))
    time6.show()

    val time7  = time6.select("jobId","sec").withColumn("lit", lit(1)).groupBy("sec")
      .agg(sum("lit"),FeatureAgg(time5.col("jobId"))).toDF("sec","parnum","jobs")
    time7.show(false)

    val time8 = time6.join(time7,"sec")
    time8.show()

    val time9 = time8.select("jobId","sql", "status", "tasks", "start", "end", "parnum", "jobs")
      .dropDuplicates().withColumn("submittime", from_unixtime(time8.col("start").cast(StringType)))
      .withColumn("endtime", from_unixtime(time8.col("end").cast(StringType)))
      .select("jobId","sql", "status", "tasks", "submittime", "endtime", "parnum", "jobs")
        .orderBy("jobId")
    time9.show()

    //time9.select("jobId","submittime", "endtime","parnum", "jobs").show(1000, false)
    val time10 = time9.groupBy("jobId","sql", "status", "tasks", "submittime", "endtime")
      .agg(max("parnum"),min("parnum"), JobAgg(time9.col("jobs")))
    time10.select("jobId", "max(parnum)", "min(parnum)", "jobagg$(jobs)").show(1000,false)

    time10.repartition(1).write.mode(SaveMode.Append).json("/home/pi/doc/history/result/history1217")

//    val output = session.read.json("/home/pi/doc/history/result/everysec")
//      .orderBy(desc("time"))
//    output.show()

//    for (i <- 1508727582 to 1511884800) {
//      val filtered = time4.filter(time4.col("start") <= i && time4.col("end") >= i)
//        .withColumn("time", lit(i))
//      if(filtered.count()!=0)
//      filtered.repartition(1).write.mode(SaveMode.Append).json("/home/pi/doc/history/result/everysec")
//      //filtered.show()
//      //println("-----------------"+(filtered.count()!=0)+"------------------")
//    }


  }

}
