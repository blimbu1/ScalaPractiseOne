package com.practise.spark

import org.apache.log4j._
import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

object StructTypeExample {

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

    val data = Seq(Row(1,"a"), Row(5, "z"))

    val schema = StructType(List(StructField("num", IntegerType, true), StructField("letter", StringType, true)))


    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.show()

    /*
    val fields = "hello,you,there".split(",").map(fieldName => StructField(fieldName,StringType,true))
    val schema = StructType(fields)
    */

    println(df.schema)

    spark.stop()
  }
}