package BikeShareAnalytics

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

/**
  * Created by kunal on 23/12/16.
  */
case class BikeData(duration: String,startDate: String,startStation: String,endDate: String,endStation: String,bike: String,memberType: String)
object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Bike Share")
      .getOrCreate()

    LogManager.getRootLogger.setLevel(Level.WARN)
    import sparkSession.implicits._

    val df = sparkSession.read.option("header","true")
      .csv("/home/kunal/Documents/Experiments/2014-Q1-cabi-trip-history-data.csv").as[BikeData]


    df.show(30)
  }
}
