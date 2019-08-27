package com.codility.challenges

import com.sun.xml.internal.ws.api.model.ExceptionType

//Trying to find the missing integer in an array. Demo of the DeutscheBank Test suite.
//see example below.
//Full text can be found in
//https://app.codility.com/demo/results/demoJ4JEKK-TAT/


object DeutscheBank {

  def solutionOne(a:Array[Int]):Int = {

    var z = 1
    var c = 0
    if (a.max < 0) return z
    try {
      for (y <- 1 to a.max toArray){
        c = c + 1
        if (!(a contains y)){
          z = y
          throw new Exception
//          scala.util.control.Exception.ignoring(classOf[ExceptionType]){
//            throw new Exception
//          }
        }
      }
    }
    catch {
      case e: Exception =>{}
    }

    if (a.length == c){
      return c+1
    }
    else {
      return z
    }

  }


  def solutionTwo(a:Array[Int]):Int = {
    var j = a.filter(_>0)
    if (j.length== 0) return 1
    var minValue = 1
    while(a contains (minValue)){
      minValue += 1
    }
    return minValue
  }

  def main(args:Array[String]):Unit = {
    println(solutionOne(Array(1,3,6,4,1,2)))// should output 5
    println(solutionOne(Array(1,2,3,4)))// should output 5
    println(solutionTwo(Array(1,2,3,4)))
    println(solutionTwo(Array(1,3,6,4,1,2)))// should output 5
    println(solutionTwo(1 to 100000 toArray))
  }

  def mainChallenge(x:Array[Int],y:Array[Int]):Int = {

    //test case 1 same x axis Array(0,0) different y axis Array(1,10)
//    --> maths.ceil(x._2 - x._1/2)

    // test case 2 same y axis  x-Axis Array(2, 12) y axis Array(5,5)
//    ---> maths.ceil(x._2 - x._1/2)

    // test case 3 different x axis different y axis . x Axis (2,4) y axis(8, 16) mapped c1 -> (2, 8) c2 -> (4,16)


    return 0

  }

}
