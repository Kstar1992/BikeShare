package BikeShareAnalytics

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession}

/**
  * Created by kunal on 26/12/16.
  */
case class BikeData(duration: Long, startDate: Timestamp, startStation: String, endDate: Timestamp, endStation: String, bike: String, memberType: String)
object DatasetCreationUtility {

  def generateAndCleanDataset(path: String,sparkSession: SparkSession ): Dataset[BikeData]= {
    import sparkSession.implicits._
    val df = sparkSession.read.option("header","true")
      .csv("/home/kunal/Documents/Experiments/2014-Q1-cabi-trip-history-data.csv")
    val filteredDf = df.na.drop().filter(!df("duration").equalTo("") && !df("startDate").equalTo("") && !df("endDate").equalTo(""))
    val timeConversionToSec = udf(timeConversionToSeconds(_:String): Long)
    filteredDf.withColumn("duration",timeConversionToSec(filteredDf("duration")))
                                          .withColumn("startDate",unix_timestamp(filteredDf("startDate"),"MM/dd/yyyy hh:mm").cast("timestamp"))
                                          .withColumn("endDate",unix_timestamp(filteredDf("endDate"),"MM/dd/yyyy hh:mm").cast("timestamp"))
                                          .withColumn("startStation",trim(filteredDf("startStation")))
                                          .withColumn("endStation",trim(filteredDf("endStation"))).as[BikeData]

  }

  def timeConversionToSeconds(duration: String): Long = {
    duration.split("[^0-9]").toList.filter(_.nonEmpty).reverse.zipWithIndex.map{case (value,index) => value.toInt*(index)*60}.sum
  }
}
