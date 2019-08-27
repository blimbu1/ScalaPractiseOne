package com.practise.spark

import java.sql.Time
import org.apache.spark.sql._
import org.apache.spark.sql.{Encoder,Encoders}
import org.apache.spark.sql.functions.{col, udf,sum}
import java.lang.Math.ceil

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction

object SkyTest {

  case class Calls(customerID:String, numberCalled:String, callDuration:String)

  def mapper(line:String):Calls = {
    val fields: Array[String] = line.split(" ")
    fields.foreach(println)
    val calls:Calls = Calls(fields(0), fields(1), fields(2))
    calls
  }

  def main(args:Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("skytest")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    import spark.implicits._
    val callList: RDD[String] = spark.sparkContext.textFile("./First/calls.log")

    val logs = callList.map(mapper).toDS()

    logs.printSchema()

    logs.select("customerID").show()

    val df = spark.read.option("delimiter"," ").option("header","false").csv("./First/calls.log")

    df.show()
    df.printSchema()
    val newColNames = Seq("customerID","numberCalled","duration")
    val newdf = df.toDF(newColNames: _*)
    newdf.show()
    newdf.printSchema()
    val timeconvert: UserDefinedFunction = udf((i:String) => {
      val fields = i.split(":")
      val totaltime: Int = fields(0).toInt * 3600 + fields(1).toInt * 60 + fields(2).toInt
      totaltime
    })

    //check out the roundings on this
    val calcCost = udf((x:Int) => {
      val cost = if (x <= 180) 0.05*x else {(x-180)*0.03 + 0.05*180}
      cost
    })

    val logswithmins: DataFrame = logs.withColumn("Time",timeconvert(col("callDuration")))
    logswithmins.show()
    val dfwithmins = newdf.withColumn("Time",timeconvert(col("duration")))
    val dfwithcost = dfwithmins.withColumn("Cost",calcCost(col("Time")))
    dfwithcost.show()
    dfwithcost.printSchema()

    val dfGroupByOne = dfwithcost.groupBy("customerID").max("Cost").as("expensive")
    val dfGroupTwo = dfwithcost.groupBy("customerID").agg(sum(col("Cost")).as("total"))

    val dfGroupThree = dfGroupTwo.join(dfGroupByOne,Seq("customerID"))
    val finaldf = dfGroupThree.withColumn("final_cost",col("total")-col("max(Cost)")).sort(col("customerID"))

    dfGroupByOne.show()
    dfGroupTwo.show()
    dfGroupThree.show()
    finaldf.show()

  }


}
