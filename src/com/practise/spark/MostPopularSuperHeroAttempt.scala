package com.practise.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark.SparkContext

import scala.io.Source
import scala.io.Codec

object MostPopularSuperHeroAttempt {

  /**

  def parsers(line:String):Map[Int, Int] = {
    val ids = line.split(" ")

    for (id <- ids){
      (id.toInt,1)
    }


  }
    **/

  def main(args: Array[String]): Unit ={

    //set the logger level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    //create a sparkcontext using everycore of the local machine.
    val sc = new SparkContext("local[*]", "SuperHeroes")

    //broadcast the Marvel-names.txt file


    //Handle character encoding issues:
    implicit  val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    /**
    //load the file Marvel-graph.txt into an RDD
    val lines = sc.textFile("./First/Marvel-graph.txt")
    **/
    val lines = Source.fromFile("./First/Marvel-graph.txt").getLines()

    var numbers = Seq[Int]()

    for (line <- lines){
      var numb = line.split(" ")
      for (number <- numb){
        numbers = numbers :+ number.toInt
      }
    }


    val distData = sc.parallelize(numbers)

    val dist = distData.map(x=> (x,1))

    val supHeroCount = dist.reduceByKey((x,y) => x + y)

    val flipped = supHeroCount.map(x => (x._2, x._1))

    val sorted = flipped.sortByKey()

    val results = sorted.collect()

    results.foreach(println)



  }

}
