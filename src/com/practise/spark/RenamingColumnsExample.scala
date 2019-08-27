package com.practise.spark

import com.practise.spark.UdfExample.betterLowerRemoveAllWhitespace
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object RenamingColumnsExample {
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

    import spark.implicits._
    val df = Seq((1L,"a","foo",3.0),(3L,"b","bar",4.0),(9L,"c","z00",5.0)).toDF

    val newNames = Seq("id","x1","x2","x3")

    val dfRenamed = df.toDF(newNames: _*)
    df.printSchema()

    dfRenamed.printSchema()

    spark.stop()
  }
}
