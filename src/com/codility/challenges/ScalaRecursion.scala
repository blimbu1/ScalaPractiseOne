package com.codility.challenges

object ScalaRecursion {

  def fibonacci(n:Int):Int = n match {
      case 0 => 0
      case 1 => 1
      case _ => fibonacci(n-1) + fibonacci(n-2)
  }

  def factorial(n:Int):Int = n match{
    case 0 | 1 => 1
    case _ => n * factorial(n-1)
  }


  def main(args:Array[String])={
    val check = 10
    println(fibonacci(check))
    println(factorial(10))
  }




}
