package com.practise.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TransformExample {

  def withGreeting(df: DataFrame): DataFrame = {
    df.withColumn("greeting", lit("hello world"))
  }

  def withFarewell(df: DataFrame): DataFrame = {
    df.withColumn("farewell", lit("goodbye"))
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    /*
    * link to article
    * https://medium.com/@mrpowers/chaining-custom-dataframe-transformations-in-spark-a39e315f903c
    * https://medium.com/@mrpowers/chaining-custom-dataframe-transformations-in-spark-a39e315f903c
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

    import spark.implicits._

    val df = Seq(
      "funny",
      "person"
    ).toDF("something")

    val weirdDf = df
      .transform(withGreeting)
      .transform(withFarewell)


    weirdDf.show()

    df
      .select()
    spark.stop()
  }
}