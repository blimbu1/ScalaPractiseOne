package com.practise.spark
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf,rand}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object UdfExampleCurrying {
  /*
  https://gist.github.com/andrearota/5910b5c5ac65845f23856b2415474c38
   */



  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Currying")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","file:///c:/temp")
      .getOrCreate()



    val rowRDD = spark.sparkContext.parallelize(0 to 999).map(Row(_))
    val schema = StructType(StructField("value",IntegerType,true):: Nil)
    val rowDF  = spark.createDataFrame(rowRDD,schema)

    var newDF = rowDF

    def hideTabooValues(taboo: List[Int]) = udf((n:Int) => if (taboo.contains(n)) -1 else n)
    val forbiddenValues = List(0,1,2)

    rowDF.select(hideTabooValues(forbiddenValues)(rowDF("value"))).show()

    newDF = newDF.withColumn("Test1",hideTabooValues(forbiddenValues)(rowDF("value")))

    println("========================")
    newDF.show()

    println("========================>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val newDFOne = newDF.withColumn("Double Vaues", rowDF("value").cast(DoubleType))

    newDFOne.show()


  }



}
