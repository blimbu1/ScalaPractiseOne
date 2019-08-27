package com.practise.spark

import org.apache.spark._
import org.apache.log4j._


object MostPopularHeroTwo {

  def main(args: Array[String]): Unit ={
    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SuperHeroTwo")

    // Read in each issue line
    val lines = sc.textFile("./First/Marvel-graph.txt")

    // Get them in the Array[Int] format

    val ids = lines.map(x => x.split(" ").map(x => x.toInt))

    val mapper = ids.map(x => (x, 1))

    val mostPopular = mapper.reduceByKey((x,y)=> x+y)

    val flipped = mostPopular.map(x => (x._2, x._1))

    val results = flipped.sortByKey()

    results.foreach(println)


  }

}
