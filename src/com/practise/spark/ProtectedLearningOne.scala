package com.practise.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf,rand}
import scala.util.control.Breaks._


object ProtectedLearningOne extends Exception{

  /**Write a function to feed to udf to sort into individual lengths*/

  def sortAccordingToLength(s:String): Option[String] = {
   if (s.length() == 4){
     return Some(s)
   }else{
     return None
   }
  }

  /** main function where the action happens*/
  def main(args:Array[String]): Unit ={

    // setting the log level to print error
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder()
      .appName("Protected")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","file:///c:/temp")
      .getOrCreate()

    /*
    val input = spark
      .read
      .format("txt")
      .option("header","true")
      .load("Z:\\Economic Stats BAU Team\\Transformation\\Developer Folders\\Binay\\enable1.txt")

    */

    //val sc = new SparkContext("local[*]", "Protected")

    import spark.implicits._

    val lines = spark.sparkContext.textFile("./First/src/resources/enable1.txt")

    val input = lines.toDF("words")

    //input.show()
    /*
    val anotherUDF = udf((clm:String) => {
      if (clm.length == 2){
        return clm
      }
      else{
        return null
      }
    })
*/
    val anotherUDF = udf[Option[String],String](sortAccordingToLength)

    //newOne.show()
    var ans = None: Option[String]
    var numberOfChoices = None: Option[Int]
    var wordLength = None: Option[Int]
    var level = None:Option[String]
    do {
      println("Enter difficulty Level: 1, 2 or 3")
      try{
        val choice = scala.io.StdIn.readLine().toInt

        choice match {
          case 1 => numberOfChoices = Some(6)
            wordLength = Some(4)
            level = Some("easy")
          case 2 => numberOfChoices = Some(10)
            wordLength = Some(5)
            level = Some("medium")
          case 3 => numberOfChoices = Some(15)
            wordLength = Some(7)
            level = Some("hard")
          case _ => throw new Exception
        }

        def selectValues(wordSize: Int) = udf((words:String) => if(words.length()==wordSize) words else null)
        /*
        This synatx is used with anotherUDF function
        var newOne = input.select(anotherUDF('words).as("easy")).na.drop()
        */
        var newOne = input.select(selectValues(wordLength.get)(input("words")).as(level.get)).na.drop()
        newOne = newOne.orderBy(rand()).limit(numberOfChoices.get)
        val answer = newOne.orderBy(rand()).limit(1).head.getString(0)
        println(answer)
        newOne.show()

        try {
          for (x <- 1 to 3) {
            println("Enter passphrase: ")
            var reply = scala.io.StdIn.readLine().trim
            if (reply == answer) {
              throw new Exception
            }
            else {
              var num = 0
              for (a <- answer) {

                if (a == reply(num)) {
                  print("X" + " ")
                }
                else {
                  print("_" + " ")
                }
                num = num + 1
              }
              println()
            }
          }
        }
        catch {
          case e: Exception => println("Correct answer")
        }

      }
      catch{
        case e: Exception => println("No such options")
      }
      println()
      println("Do you want to quit ? yes to quit!!")

      ans = Some(scala.io.StdIn.readLine().trim)
    }while(ans.get != "yes")


  }
}

/* Improvment tips. sort the words according to length early on using udfs
  Use RDDs to generate RDD samples
  Try and implement regex
 */