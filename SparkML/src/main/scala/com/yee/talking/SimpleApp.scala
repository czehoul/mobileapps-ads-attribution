package com.yee.talking

import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/machineLearning-ext/spark/README.md" // Should be some file on your system
    //this line is replace to run on local eclipse
    //val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val spark = SparkSession.builder.appName("Simple Application").master("local").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop() 
  }
}