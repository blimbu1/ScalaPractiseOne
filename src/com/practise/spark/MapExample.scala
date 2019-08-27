package com.practise.spark

object MapExample {

  def f(x:Int): Option[Int] = if (x > 2) Some(x) else None
  
  def g(v:Int): Seq[Int] = List(v-1,v,v+1)

  def main(args:Array[String])={
    val names: Seq[Seq[Seq[String]]] = Seq("clay","concrete","sandlime").map{
      name => Seq("hello","world").map{
        filler => Seq("china","India","Pak").map{
          tootle => s"${name}_${filler}_${tootle}"
        }
      }
    }

    names.foreach(println)

    val secondnames = Seq("clay","concrete","sandlime").flatMap{
      name => Seq("hello","world").flatMap{
        filler => Seq("china","India","Pak").map{
          tootle => s"${name}_${filler}_${tootle}"
        }
      }
    }

    secondnames.foreach(println)

  }



}
