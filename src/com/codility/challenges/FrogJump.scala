package com.codility.challenges

object FrogJump {

  def solution(x:Int,y:Int,d:Int):Int={

    if (x == y) return 0

    if (y <= (2*x)) return 1

    val c = (y - x)/d

    if ((y-x)%d >0) {
      return c+1
    }
    else
    {
      return c
    }

  }

  def main(args:Array[String]): Unit ={
    println(solution(10,85,30))
  }
}
