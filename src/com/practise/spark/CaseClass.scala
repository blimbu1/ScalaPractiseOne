package com.practise.spark

import org.apache.spark.sql.SparkSession

object CaseClass {

  case class FlightMetadata(count:BigInt, randomData: BigInt)

  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    import spark.implicits._

    val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
      .withColumnRenamed("_1","count").withColumnRenamed("_2","randomData")
      .as[FlightMetadata] //without the as flightsMeta is a sql.Dataframe  , with the .as[FlightMetadata] flightsMeta
    //is a Dataset[CaseClass.FlightMetadata]

    flightsMeta.show()

    flightsMeta.printSchema()

  }

}
