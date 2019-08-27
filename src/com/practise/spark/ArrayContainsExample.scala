package com.practise.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.array_contains


object ArrayContainsExample {


  def main(args: Array[String]): Unit ={

    //    https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-functions-collection.html#array_contains

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    /*
    val  c = array_contains(column=$"ids", value=1)

    import spark.implicits._

    val ids = Seq(Seq(1,2,3), Seq(1),Seq(2,3)).toDF("ids")

    val q = ids.filter(c)

    q.show()
    */
//    https://stackoverflow.com/questions/40134975/selecting-a-range-of-elements-in-an-array-spark-sql/40136386
    /*
    import spark.implicits._
    val data1 = Seq(List("Jon","Snow","Castle","Black","Hero"),List("Ned","is","No")).toDF("emp_details")
    data1.show()
    val input = sqlContext.sql("select emp_details from emp_det")
  */
  }
}
