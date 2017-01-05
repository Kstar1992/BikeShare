package BikeShareAnalytics

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
/**
  * Created by kunal on 23/12/16.
  */
object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Bike Share")
      .getOrCreate()

    LogManager.getRootLogger.setLevel(Level.WARN)
    val filePath = "src/main/resources/InputData/Bike_Trip_Data.csv"
    val inputDS = DatasetCreationUtility.generateAndCleanDataset(filePath,sparkSession)
    val ml_bike = MachineLearningModels.dataPreparationForML(sparkSession,inputDS)
    val splits = ml_bike.randomSplit(Array(0.8,0.2),seed = 11L)
    val trainSet = splits(0).rdd.cache()
    val testSet = splits(1).rdd
    MachineLearningModels.naiveBayesAlgo(sparkSession,trainSet,testSet)
    MachineLearningModels.decisionTreeAlgo(sparkSession,trainSet,testSet)
    MachineLearningModels.randomForest(sparkSession,trainSet,testSet)
  }

}
