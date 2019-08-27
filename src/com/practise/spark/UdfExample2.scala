package com.practise.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object UdfExample2 {

  /*Find link for this tutorial @
  https://alvinhenrick.com/2016/07/10/apache-spark-user-defined-functions/
   */

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

    val testDF = List(("Michael",15,1.0),("Andy",30,1.0),("Justin",19,1.0)).toDF("name","age","sal")
    testDF.show()
    testDF.printSchema()

    val addOne: Int => Int = (i:Int)=> i + 1

    val addOneByFunction: UserDefinedFunction = udf(addOne)

    testDF.select(addOneByFunction($"age") as "test1D").show()

    // Works With Sql Expr and DSL API
    val addOneByRegister = spark.sqlContext.udf.register("addOneByRegister", addOne)

    //SQL
    testDF.selectExpr("addOneByRegister(age) as test2S").show

    //DSL
    testDF.select(addOneByRegister($"age") as "test2D").show

    //DSL by callUDF
    testDF.select(callUDF("addOneByRegister", $"age") as "test2CallUDF").show

    // How to pass literal values to UDF ?

    val addByLit: UserDefinedFunction = udf((i: Int, x: Int) => x + i)

    //DSL `lit` function
    testDF.select(addByLit(functions.lit(3), $"age") as "testLitF").show

    def addByCurryFunction(i: Int) = udf((x: Int) => x + i)

    //DSL literal value via CURRYING
    testDF.select(addByCurryFunction(2)($"age") as "testLitC").show

    def addByCurry(i: Int) = (x: Int) => x + i

    val addByCurryRegister = spark.sqlContext.udf.register("addByCurryRegister", addByCurry(2))

    //SQL literal value via CURRYING
    testDF.selectExpr("addByCurryRegister(age) as testLitC1").show

    //DSL literal value via CURRYING
    testDF.select(addByCurryRegister($"age") as "testLitC2").show

    //More than 22 UDF Argument
    val udfWith23Arg = udf((array: Seq[Double]) => array.sum)

    //DSL Example
    testDF.select(udfWith23Arg(array($"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal",
      $"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal",
      $"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal")) as "total").show
    spark.stop()
  }
}