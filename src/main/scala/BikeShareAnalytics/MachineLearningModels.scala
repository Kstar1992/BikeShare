package BikeShareAnalytics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
/**
  * Created by kunal on 26/12/16.
  */
object MachineLearningModels {

  def dataPreparationForML(sparkSession:SparkSession,input: Dataset[BikeData]): Dataset[LabeledPoint] = {
    import sparkSession.implicits._
    val stationMap = input.map(_.startStation).union(input.map(_.endStation)).distinct.rdd.zipWithUniqueId().collectAsMap()
    val memberTypeMap = Map("Registered" -> 0.0, "Casual" -> 1.0)
    input.map{ trip =>
      val s0 = stationMap.getOrElse(trip.startStation, 0L).toDouble
      val s1 = stationMap.getOrElse(trip.endStation, 0L).toDouble
      LabeledPoint(memberTypeMap(trip.memberType), Vectors.dense(trip.duration,s0,s1))
    }
  }

  def naiveBayesAlgo(sparkSession: SparkSession,trainSet: RDD[LabeledPoint],testSet: RDD[LabeledPoint]) = {
    val n = testSet.count()
    val model = NaiveBayes.train(trainSet)
    val pred = testSet.map(t => (model.predict(t.features), t.label))
    val cm = sparkSession.sparkContext.parallelize(pred.countByValue().toSeq)
    val cm_nb = cm.map(x => (x._1, (1.0*x._2/n))).sortBy(_._1, true)
    cm_nb.foreach(println)
  }

  def decisionTreeAlgo(sparkSession: SparkSession,trainSet: RDD[LabeledPoint],testSet: RDD[LabeledPoint]) = {
    val n = testSet.count()
    val numClasses = 2
    val categoricalFearuresInfo = Map[Int,Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainSet, numClasses, categoricalFearuresInfo, impurity, maxDepth,maxBins)
    val pred = testSet.map(t => (model.predict(t.features), t.label))
    val cm = sparkSession.sparkContext.parallelize(pred.countByValue().toSeq)
    val cm_nb = cm.map(x => (x._1, (1.0*x._2/n))).sortBy(_._1, true)
    cm_nb.foreach(println)
  }
   def randomForest(sparkSession: SparkSession,trainSet: RDD[LabeledPoint],testSet: RDD[LabeledPoint]) = {
     val n = testSet.count()
     val numClasses = 2
     val categoricalFearuresInfo = Map[Int,Int]()
     val numTrees = 6
     val featureSubsetStratergy = "auto"
     val impurity = "gini"
     val maxDepth = 5
     val maxBins = 32
     val model= RandomForest.trainClassifier(trainSet,numClasses,categoricalFearuresInfo,numTrees,featureSubsetStratergy,impurity,maxDepth,maxBins)
     val pred = testSet.map(t => (model.predict(t.features), t.label))
     val cm = sparkSession.sparkContext.parallelize(pred.countByValue().toSeq)
     val cm_nb = cm.map(x => (x._1, (1.0*x._2/n))).sortBy(_._1, true)
     cm_nb.foreach(println)
   }
}
