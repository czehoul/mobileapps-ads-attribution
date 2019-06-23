package com.yee.talking

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.param.shared.HasOutputCols
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.sql.functions._

/**
 * Transform Hour of the day to sin and cos of the the hour,
 * receive timestamp as input
 */
class HourOfDayTransformer (override val uid: String) extends Transformer 
  with HasInputCol with HasOutputCols with DefaultParamsWritable {
   
  def this() = this(Identifiable.randomUID("hourOfDayTransformer"))
  
  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value) 

  def copy(extra: ParamMap): HourOfDayTransformer = {
    defaultCopy(extra)
  }
  
  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a integer
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != TimestampType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type TimestampType")
    }    
    // Add the return field
    schema.add(StructField($(outputCols)(0), DoubleType, false))
      .add(StructField($(outputCols)(1), DoubleType, false))    
  }

  def transform(df: Dataset[_]): DataFrame = {
    df.withColumn("__hourtemp__", 
        hour((round(unix_timestamp(df.col($(inputCol)))/3600)*3600).cast("timestamp")))
      .select(col("*"), 
        sin(col($(inputCol)).cast("double") * (2.0*math.Pi/24)).as($(outputCols)(0)),
        cos(col($(inputCol)).cast("double") * (2.0*math.Pi/24)).as($(outputCols)(1)))
      .drop("__hourtemp__")
  }

}