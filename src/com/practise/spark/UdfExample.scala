package com.practise.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.log4j._

object UdfExample {

  def betterLowerRemoveAllWhitespace(s: String): Option[String] = {
    val str = Option(s).getOrElse(return None)
    Some(str.toLowerCase().replaceAll("\\s", ""))
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    //function variable
    val betterLowerRemoveAllWhitespaceUDF = udf[Option[String], String](betterLowerRemoveAllWhitespace)

    import spark.implicits._

    val anotherDF = List((" BOO "), (" HOO "), (null)).toDF("cry")

    anotherDF.show

    anotherDF.select(betterLowerRemoveAllWhitespaceUDF('cry).as("clean_cry")).show

    anotherDF.select(betterLowerRemoveAllWhitespaceUDF('cry).as("clean_cry")).printSchema()

    spark.stop()
  }
}