
name := "BikeShare"

version := "1.0"

scalaVersion := "2.11.7"

val sparkVersion  = "2.0.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion

//libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"