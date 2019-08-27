package com.practise.spark

import org.apache.log4j._
import org.apache.spark._

object PurchaseByCustomerSorted {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val custNumber = fields(0).toInt
    val price = fields(2).toFloat
    (custNumber, price)
  }

  def main(args: Array[String]): Unit ={

    //set the log level to print only errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //create a sparkcontext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerPurchase")

    //load each line of the source file into an RDD
    val input = sc.textFile("./First/customer-orders.csv")

    //use parseline function to convert to (customerNumber, price) tuple
    val rdd1 = input.map(parseLine)


    // add all the common customer numbers using reduceByKey
    val customerSpend = rdd1.reduceByKey((x,y)=> x + y)

    //Flipping the key value pair so that the value is now our key
    val makeValueKey = customerSpend.map(x => (x._2, x._1))

    //Sorting of find our most expensive customer
    val sorts = makeValueKey.sortByKey()

    // collect the result in a format that we can work with
    val results = sorts.collect()

    // sort and print the final result.
    results.foreach(println)

  }

}
