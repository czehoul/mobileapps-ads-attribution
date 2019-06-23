package com.yee.talking

import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import breeze.linalg._

/**
  * Distributed Hyperparameter tuning in Spark
  */
object TalkingDataTuningApp {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {

      println("Require application arguments FILE_PATH, MODEL_PATH, CHECKPOINT_PATH")

    } else {


      val filePath = args(0)
      val modelPath = args(1)
      val checkPointPath = args(2)
      val numWorker = 4

      //val spark = SparkSession.builder.appName("TalkingDataModel Application").master("local[*]").getOrCreate()
      val spark = SparkSession.builder.appName("TalkingDataModel Tuning Application").getOrCreate()

      val schema = new StructType(Array(
        StructField("cat_vec_0", DoubleType, false),
        StructField("cat_vec_1", DoubleType, false),
        StructField("cat_vec_2", DoubleType, false),
        StructField("cat_vec_3", DoubleType, false),
        StructField("cat_vec_4", DoubleType, false),
        StructField("cat_vec_5", DoubleType, false),
        StructField("cat_vec_6", DoubleType, false),
        StructField("cat_vec_7", DoubleType, false),
        StructField("cat_vec_8", DoubleType, false),
        StructField("cat_vec_9", DoubleType, false),
        StructField("cat_vec_10", DoubleType, false),
        StructField("cat_vec_11", DoubleType, false),
        StructField("cat_vec_12", DoubleType, false),
        StructField("cat_vec_13", DoubleType, false),
        StructField("cat_vec_14", DoubleType, false),
        StructField("cat_vec_15", DoubleType, false),
        StructField("cat_vec_16", DoubleType, false),
        StructField("cat_vec_17", DoubleType, false),
        StructField("cat_vec_18", DoubleType, false),
        StructField("cat_vec_19", DoubleType, false),
        StructField("cat_vec_20", DoubleType, false),
        StructField("cat_vec_21", DoubleType, false),
        StructField("cat_vec_22", DoubleType, false),
        StructField("cat_vec_23", DoubleType, false),
        StructField("cat_vec_24", DoubleType, false),
        StructField("click_time", TimestampType, false),
        StructField("is_attributed", IntegerType, false),
        StructField("cumcount_by_ip_app_past_5min", IntegerType, true),
        StructField("cumcount_by_ip_app_past_1hr_to_8hr", IntegerType, true),
        StructField("cumcount_by_ip_app_os_past_5min", IntegerType, true),
        StructField("cumcount_by_ip_app_os_past_5min_to_1hr", IntegerType, true),
        StructField("cumcount_by_ip_app_os_past_1hr_to_8hr", IntegerType, true),
        StructField("cumcount_by_ip_device_os_past_5min", IntegerType, true),
        StructField("cumcount_by_ip_device_os_past_5min_to_1hr", IntegerType, true),
        StructField("cumcount_by_ip_device_os_past_1hr_to_8hr", IntegerType, true),
        StructField("cumcount_by_ip_device_os_app_channel_past_5min", IntegerType, true),
        StructField("cumcount_by_ip_device_os_app_channel_past_5min_to_1hr", IntegerType, true),
        StructField("cumcount_by_ip_device_os_app_channel_past_1hr_to_8hr", IntegerType, true),
        StructField("unique_channel_by_ip_cum_count", IntegerType, true),
        StructField("unique_app_by_ip_device_os_cum_count", IntegerType, true),
        StructField("unique_app_by_ip_cum_count", IntegerType, true),
        StructField("unique_os_by_ip_app_cum_count", IntegerType, true),
        StructField("unique_device_by_ip_cum_count", IntegerType, true),
        StructField("cumcount_by_ip_past_5min", IntegerType, true),
        StructField("cumcount_by_ip_past_5min_to_1hr", IntegerType, true),
        StructField("cumcount_by_ip_past_1hr_to_8hr", IntegerType, true)))

      val rawInput = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("timestampFormat", "yyyy-MM-dd HH:mm:ss z").schema(schema).load(filePath);

      val Array(training, test) = rawInput.randomSplit(Array(0.8, 0.2), 123)

      val hourOfDayTransformer = new HourOfDayTransformer()
        .setInputCol("click_time")
        .setOutputCols(Array("sin_hour", "cos_hour"))
      val logTransformer = new LogTransformer()
        .setInputCols(Array(
          "cumcount_by_ip_app_past_5min",
          "cumcount_by_ip_app_past_1hr_to_8hr",
          "cumcount_by_ip_app_os_past_5min",
          "cumcount_by_ip_app_os_past_5min_to_1hr",
          "cumcount_by_ip_app_os_past_1hr_to_8hr",
          "cumcount_by_ip_device_os_past_5min",
          "cumcount_by_ip_device_os_past_5min_to_1hr",
          "cumcount_by_ip_device_os_past_1hr_to_8hr",
          "cumcount_by_ip_device_os_app_channel_past_5min",
          "cumcount_by_ip_device_os_app_channel_past_5min_to_1hr",
          "cumcount_by_ip_device_os_app_channel_past_1hr_to_8hr",
          "cumcount_by_ip_past_5min",
          "cumcount_by_ip_past_5min_to_1hr",
          "cumcount_by_ip_past_1hr_to_8hr"))
      val vectorAssembler = new VectorAssembler()
        .setInputCols(Array("cat_vec_0", "cat_vec_1", "cat_vec_2", "cat_vec_3", "cat_vec_4",
          "cat_vec_5", "cat_vec_6", "cat_vec_7", "cat_vec_8", "cat_vec_9",
          "cat_vec_10", "cat_vec_11", "cat_vec_12", "cat_vec_13", "cat_vec_14",
          "cat_vec_15", "cat_vec_16", "cat_vec_17", "cat_vec_18", "cat_vec_19",
          "cat_vec_20", "cat_vec_21", "cat_vec_22", "cat_vec_23", "cat_vec_24",
          "sin_hour", "cos_hour", "cumcount_by_ip_app_past_5min",
          "cumcount_by_ip_app_past_1hr_to_8hr", "cumcount_by_ip_app_os_past_5min",
          "cumcount_by_ip_app_os_past_5min_to_1hr", "cumcount_by_ip_app_os_past_1hr_to_8hr",
          "cumcount_by_ip_device_os_past_5min", "cumcount_by_ip_device_os_past_5min_to_1hr",
          "cumcount_by_ip_device_os_past_1hr_to_8hr", "cumcount_by_ip_device_os_app_channel_past_5min",
          "cumcount_by_ip_device_os_app_channel_past_5min_to_1hr", "cumcount_by_ip_device_os_app_channel_past_1hr_to_8hr",
          "cumcount_by_ip_past_5min", "cumcount_by_ip_past_5min_to_1hr",
          "cumcount_by_ip_past_1hr_to_8hr"))
        .setOutputCol("features")

      val scaler = new StandardScaler()
        .setInputCol(vectorAssembler.getOutputCol)
        .setOutputCol("scaledFeatures")
        .setWithStd(true)
        .setWithMean(true)

      val weightRatio = (training.filter(col("is_attributed") === 0).count.toFloat / training.filter(col("is_attributed") === 1).count.toFloat)

      val xgbClassifier = new XGBoostClassifier()
        .setFeaturesCol(scaler.getOutputCol)
        .setLabelCol("is_attributed")
        .setScalePosWeight(weightRatio) //looks like we hv no option to set this dynamically in CV for spark
        .setNumRound(200)
        .setEvalMetric("auc")
        .setNumEarlyStoppingRounds(10)
        .setNumWorkers(numWorker)
        .setMaximizeEvaluationMetrics(true)
        .setObjective("binary:logistic")
        .setCheckpointInterval(10)
        .setCheckpointPath(checkPointPath)

      val paramGrid = new ParamGridBuilder()
        .addGrid(xgbClassifier.maxDepth, Array(5, 6, 7, 8, 9, 10, 11, 12))
        .addGrid(xgbClassifier.eta, Array(0.01, 0.1, 0.3))
        .addGrid(xgbClassifier.minChildWeight, Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
        .addGrid(xgbClassifier.alpha, linspace(0.0, 1.0, 5).toArray)
        .addGrid(xgbClassifier.lambda, linspace(0.0, 1.0, 5).toArray)
        .addGrid(xgbClassifier.subsample, linspace(0.5, 1.0, 5).toArray)
        .addGrid(xgbClassifier.colsampleBytree, linspace(0.5, 1.0, 5).toArray)
        .addGrid(xgbClassifier.maxDepth, Array(5))
        .build()

      val pipeline = new Pipeline()
        .setStages(Array(hourOfDayTransformer, logTransformer, vectorAssembler, scaler, xgbClassifier))

      val evaluator = new BinaryClassificationEvaluator()
        .setMetricName("areaUnderROC")
        .setRawPredictionCol("rawPrediction")
        .setLabelCol("is_attributed")

      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(3)


      val cvModel = cv.fit(training)
      val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(4)
        .asInstanceOf[XGBoostClassificationModel]

      //Get best params
      print(bestModel.extractParamMap())

      spark.stop()

    }
  }

}