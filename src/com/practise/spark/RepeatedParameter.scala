package com.practise.spark
import org.apache.log4j._




object RepeatedParameter {

//    1. All repeated parameters must be of the same type
//    2. Can only have one argument as repeated parameter in the method definition
//    3. Scala allows only last parameter of method call to be repeated.

  def showMeParameters(args:String*) = {
    //    When we do this this function takes acts like it takes an Array of String
    //    This enables us to use all the helper functions of arrays. Like in the example
    //    below foreach
    args.foreach(println)
  }

  def gradeCalculator(scores: Int*):String = {
    val average = scores.sum/scores.length
    var grading = None:Option[String]

    average match {
      case gradeA if average > 70 => "A"
      case gradeC if average < 30 => "C"
      case _ => "B"
    }

  }

  def main(args: Array[String]): Unit ={

    //    https://dzone.com/articles/scala-repeated-method-parameters

    Logger.getLogger("org").setLevel(Level.ERROR)

    showMeParameters("repeated","parameters","scala","learning")

    showMeParameters()

    gradeCalculator(40,50,80,90)

//    passing gradeCalculator(Array(80,90,100)) will give us a type mismatch error.
    val arrayexample = Array(80,90,100)

    gradeCalculator(arrayexample: _*).foreach(println)
//    : _* tells to pass the input one at a time, instead of passing the whole array

  }
}
