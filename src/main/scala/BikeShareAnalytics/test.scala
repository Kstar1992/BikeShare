package BikeShareAnalytics

import org.apache.commons.lang.math.NumberUtils.min
import org.apache.spark.sql.SparkSession

/**
  * Created by kunal on 26/12/16.
  */
object test {
  def main(args: Array[String]): Unit = {

//    val sparkSession = SparkSession
//      .builder()
//      .master("local")
//      .appName("Bike Share")
//      .getOrCreate()

//    val df = sparkSession.read.option("header", "true")
//      .csv("/home/kunal/Downloads/RXCOEFF2017.csv")
//      println(customLevenshteinDistance("MARINA","MARISSA"))

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Bike Share")
      .getOrCreate()

                                                                                                                                                                                                                                                                                                                                                                                                              val df = sparkSession.read.option("header", "true")
                                                                                                                                                                                                                                                                                                                                                                                                                .csv("/home/kunal/Downloads/ESRDCNTLOUT.csv")
                                                                                                                                                                                                                                                                                                                                                                                                              val sch = df.schema
                                                                                                                                                                                                                                                                                                                                                                                                              val count = df.select("FMTNAME").distinct().map(x => x.get(0).toString).collect().toList

                                                                                                                                                                                                                                                                                                                                                                                                              count.foreach{x => val ndf = df.filter(df("FMTNAME") === x).select("START","LABEL")
                                                                                                                                                                                                                                                                                                                                                                                                                ndf.filter(x=> x.getString(0).contains("OTHER")!=1)
                                                                                                                                                                                                                                                                                                                                                                                                                ndf.write.option("header","true").save("/home/kunal/Desktop/CMSESRD/"+x)
                                                                                                                                                                                                                                                                                                                                                                                                              }

    System.exit(0)
  }
  def customLevenshteinDistance(lhs:String, rhs: String):Double= {
    val distance = Array.ofDim[Double](lhs.length() + 1, rhs.length() + 1)
    var cost_i:Double=0.0
    var cost_j:Double=0.0

    for ( i <- 0 to lhs.length()){
      distance(i)(0) = i
    }

    for (j <- 0 to rhs.length())
      distance(0)(j) = j

    for (i <- 1 to lhs.length()){
      for (j <-1 to rhs.length()) {
        if (i < lhs.length() / 4) {
          cost_i = 1
        }
        else if (i >= lhs.length() / 4  && i < lhs.length()/2) {
          cost_i = 0.85
        }
        else if (i >= lhs.length() / 2  && i < lhs.length() * 3/4) {
          cost_i = 0.7
        }
        else {
          cost_i = 0.6
        }

        if (j < rhs.length() / 4) {
          cost_j = 1
        }
        else if (j >= rhs.length() / 4  && j < rhs.length()/2) {
          cost_j = 0.85
        }
        else if (j >= rhs.length() / 2  && j < rhs.length() * 3/4) {
          cost_j = 0.7
        }
        else {
          cost_j = 0.6
        }

        distance(i)(j) = min(
          distance(i - 1)(j) + cost_i,
          distance(i)(j - 1) + cost_j,
          distance(i - 1)(j - 1) + (if(lhs.charAt(i - 1) == rhs.charAt(j - 1))  0 else  cost_i))
      }
    }
    1 - (distance(lhs.length())(rhs.length())*1.0/Math.min(lhs.length(),rhs.length()))
  }
}