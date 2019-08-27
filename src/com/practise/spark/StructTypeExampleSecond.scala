package com.practise.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col,udf,struct}

object StructTypeExampleSecond {

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
      Row(20.0, "dog"),
      Row(3.5, "cat"),
      Row(0.0006, "ant")
    )

    val schema = StructType(
      List(
        StructField("weight", DoubleType, true),
        StructField("animal_type", StringType, true)))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    df.show()

    println(df.schema)

    val actualDF = df.withColumn(
      "animal_interpretation",
      /*
      * struct function def found @
      * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$@struct%28cols:org.apache.spark.sql.Column*%29:org.apache.spark.sql.Column
      * return a new struct column(takes column as param)
      * */
      struct(
        (col("weight") > 5).as("is_large_animal"),
        col("animal_type").isin("rat","cat","dog").as("is_mammal")
      )
    )

    actualDF.show()
    actualDF.show(truncate = false)
    println(actualDF.schema)

    actualDF.select(
      col("animal_type"),
      col("animal_interpretation")("is_large_animal")
        .as("is_large_animal"),
      col("animal_interpretation")("is_mammal")
        .as("is_mammal")
    ).show(truncate = false)


    spark.stop()
  }
}