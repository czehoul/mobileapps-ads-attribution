package com.yee.talking

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline

object MainTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local").getOrCreate()
    val df = spark.createDataFrame(Seq(
      (1, 1, 2, 3, 8, 4, 5),
      (2, 4, 3, 8, 7, 9, 8),
      (3, 6, 1, 9, 2, 3, 6),
      (4, 10, 8, 6, 9, 4, 5),
      (5, 9, 2, 7, 10, 7, 3),
      (6, 1, 1, 4, 2, 8, 4))).toDF("id1", "id2", "id3", "id4", "id5", "id6", "id7")

    val assembler1 = new VectorAssembler().
      setInputCols(Array("id2", "id3", "id4")).
      setOutputCol("features")
    
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)
      
    val pipeline = new Pipeline()
      .setStages(Array(assembler1, scaler))

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = pipeline.fit(df)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(df)
    scaledData.show()

  }
}