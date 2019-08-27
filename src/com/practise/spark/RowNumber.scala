package com.practise.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object RowNumber {

  case class noaaData(zip:String,station:String,date:Long,value:String=null,distance:Int)

  def main(args: Array[String])={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val t = Seq(
      noaaData("99900","A",20160601,".2",   5),
      noaaData("99901","B",20160601,".3",   3),
      noaaData("99902","C",20160601,".1",   3),
      noaaData("99903","D",20160601,".01",  3),
      noaaData("99904","E",20160601,".12",  2),
      noaaData("99905","F",20160601,".13",  2),
      noaaData("99906","G",20160601,".2",   1),
      noaaData("99907","H",20160601,".3",   4),
      noaaData("99908","I",20160601,".01",  5),
      noaaData("99909","J",20160601,".1",   2),
      noaaData("99910","K",20160601,".4",   3),
      noaaData("99900","S",20160601,".2",   1),
      noaaData("99901","X",20160601,".3",   4),
      noaaData("99902","P",20160601,".1",   1),
      noaaData("99903","W",20160601,".01",  1),
      noaaData("99904","R",20160601,".12",  4),
      noaaData("99905","A",20160601,".13",  4),
      noaaData("99906","L",20160601,".2",   4),
      noaaData("99907","T",20160601,null,   3),
      noaaData("99908","O",20160601,".01",  2),
      noaaData("99909","F",20160601,".1",   4),
      noaaData("99910","Z",20160601,".4",   4),
      noaaData("99900","X",20160601,".2",   3),
      noaaData("99901","I",20160601,".3",   2),
      noaaData("99902","P",20160601,".1",   4),
      noaaData("99903","E",20160601,".01",  2),
      noaaData("99904","Z",20160601,".12",  2),
      noaaData("99905","L",20160601,".13",  5),
      noaaData("99906","Y",20160601,".2",   4),
      noaaData("99907","T",20160601,".3",   1),
      noaaData("99908","K",20160601,null,   2),
      noaaData("99909","F",20160601,".1",   2),
      noaaData("99910","C",20160601,".4",   1),

      noaaData("99900","A",20160602,".2",   5),
      noaaData("99901","B",20160602,".3",   3),
      noaaData("99902","C",20160602,".1",   3),
      noaaData("99903","D",20160602,".01",  3),
      noaaData("99904","E",20160602,".12",  2),
      noaaData("99905","F",20160602,".13",  2),
      noaaData("99906","G",20160602,".2",   1),
      noaaData("99907","H",20160602,".3",   4),
      noaaData("99908","I",20160602,".01",  5),
      noaaData("99909","J",20160602,".1",   2),
      noaaData("99910","K",20160602,".4",   3),
      noaaData("99900","S",20160602,".2",   1),
      noaaData("99901","X",20160602,".3",   4),
      noaaData("99902","P",20160602,".1",   1),
      noaaData("99903","W",20160602,".01",  1),
      noaaData("99904","R",20160602,".12",  4),
      noaaData("99905","A",20160602,".13",  4),
      noaaData("99906","L",20160602,".2",   4),
      noaaData("99907","T",20160602,null,   3),
      noaaData("99908","O",20160602,".01",  2),
      noaaData("99909","F",20160602,".1",   4),
      noaaData("99910","Z",20160602,".4",   4),
      noaaData("99900","X",20160602,".2",   3),
      noaaData("99901","I",20160602,".3",   2),
      noaaData("99902","P",20160602,".1",   4),
      noaaData("99903","E",20160602,".01",  2),
      noaaData("99904","Z",20160602,".12",  2),
      noaaData("99905","L",20160602,".13",  5),
      noaaData("99906","Y",20160602,".2",   4),
      noaaData("99907","T",20160602,".3",   1),
      noaaData("99908","K",20160602,null,   2),
      noaaData("99909","F",20160602,".1",   2),
      noaaData("99910","C",20160601,".4",   1),

      noaaData("99900","A",20160603,".2",   5),
      noaaData("99901","B",20160603,".3",   3),
      noaaData("99902","C",20160603,".1",   3),
      noaaData("99903","D",20160603,".01",  3),
      noaaData("99904","E",20160603,".12",  2),
      noaaData("99905","F",20160603,".13",  2),
      noaaData("99906","G",20160603,".2",   1),
      noaaData("99907","H",20160603,".3",   4),
      noaaData("99908","I",20160603,".01",  5),
      noaaData("99909","J",20160603,".1",   2),
      noaaData("99910","K",20160603,".4",   3),
      noaaData("99900","S",20160603,null,   1),
      noaaData("99901","X",20160603,".3",   4),
      noaaData("99902","P",20160603,".1",   1),
      noaaData("99903","W",20160603,".01",  1),
      noaaData("99904","R",20160603,".12",  4),
      noaaData("99905","A",20160603,".13",  4),
      noaaData("99906","L",20160603,".2",   4),
      noaaData("99907","T",20160603,null,   3),
      noaaData("99908","O",20160603,".01",  2),
      noaaData("99909","F",20160603,".1",   4),
      noaaData("99910","Z",20160603,".4",   4),
      noaaData("99900","X",20160603,".2",   3),
      noaaData("99901","I",20160603,".3",   2),
      noaaData("99902","P",20160603,".1",   4),
      noaaData("99903","E",20160603,".01",  2),
      noaaData("99904","Z",20160603,".12",  2),
      noaaData("99905","L",20160603,".13",  5),
      noaaData("99906","Y",20160603,".2",   4),
      noaaData("99907","T",20160603,".3",   1),
      noaaData("99908","K",20160603,null,   2),
      noaaData("99909","F",20160603,".1",   2),
      noaaData("99910","C",20160603,".4",   1))

    import spark.implicits._

    val df = spark.sqlContext.createDataFrame(t)


    //Example using Row_Number() Window Function

    // number the rows by ascending distance from each zip, filtering out null values
    val numbered = df.filter("value is not null").withColumn("rank", row_number().over(Window.partitionBy("zip","date").orderBy("distance")))

    // show data
    numbered.select("*").orderBy("date", "zip", "distance", "station").show(100)

    // show just the top rows.
    numbered.select("*").where("rank = 1").orderBy("date", "zip").show(100)



    //Example using Rank() Window Function

    // rank the rows by ascending distance from each zip, filtering out null values
    val ranked = df.filter("value is not null").withColumn("rank", rank().over(Window.partitionBy("zip","date").orderBy("distance")))

    // show data
    ranked.select("*").orderBy("date", "zip", "distance", "station").show(100)

    // Note duplicate zip|station rows. In this case it doesn't matter just pick one'
    // show just the top rows.
    ranked.select("*").where("rank = 1").orderBy("date", "zip").show(100)



  }


}
