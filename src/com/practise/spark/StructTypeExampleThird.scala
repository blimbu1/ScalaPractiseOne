package com.practise.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StructTypeExampleThird {

  def withIsTeenager() (df:DataFrame): DataFrame = {
    df.withColumn("is_teenager", col("age").between(13,19))
  }

  def withHasPositiveMood()(df: DataFrame): DataFrame={
    df.withColumn(
      "has_positive_mood",
      col("mood").isin("happy", "glad")
    )
  }

  def withWhatToDo()(df: DataFrame)={
    df.withColumn(
      "what_to_do",
      when(
        col("is_teenager") && col("has_positive_mood"),
        "have a chat"
      )
    )
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    /*
    * https://medium.com/@mrpowers/adding-structtype-columns-to-spark-dataframes-b44125409803
    * */

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val data = Seq(
      Row(30, "happy"),
      Row(13, "sad"),
      Row(18, "glad")
    )

    val schema = StructType(
      List(
        StructField("age", IntegerType, true),
        StructField("mood", StringType, true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
/*
    df
      .transform(ExampleTransforms.withIsTeenager())
      .transform(ExampleTransforms.withHasPositiveMood())
      .transform(ExampleTransforms.withWhatToDo())
      .show()


*/
    spark.stop()
  }
}