package com.yee.talking

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.param.shared.HasOutputCols
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.sql.functions
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.types.NumericType
import org.apache.spark.ml.util.SchemaUtils

/**
 * Logarithm Transformer
 */
class LogTransformer (override val uid: String) extends Transformer 
  with HasInputCols with HasOutputCols with DefaultParamsWritable {
  
  def this() = this(Identifiable.randomUID("logTransformer"))
  
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def copy(extra: ParamMap): LogTransformer = {
    defaultCopy(extra)
  }
  
  override def transformSchema(schema: StructType): StructType = {
    //All fields should be numeric
    val fields = schema($(inputCols).toSet)
    fields.foreach { fieldSchema =>
      val dataType = fieldSchema.dataType
      val fieldName = fieldSchema.name
      require(dataType.isInstanceOf[NumericType], 
          s"${dataType} does not match numberic type")
    } 
    return schema
  }
    
  def transform(df: Dataset[_]): DataFrame = {
    $(inputCols).toSeq.foldLeft(df.toDF()){(tempDF, colName) => 
      tempDF.withColumn(colName, functions.log(tempDF.col(colName) + 1))}
  }
  
}