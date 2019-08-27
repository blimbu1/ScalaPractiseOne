package com.codility.challenges

/*
Challenge can be found in --->https://app.codility.com/programmers/lessons/1-iterations/binary_gap/
Descripton: Challenge involves finding the biggest binary gap ~ number of zeroes between 1.
Example : 1041 ~ 10000010001 should return 5
 */

object Solution {

  def solution(n: Int): Int = {
    // write your code in Scala 2.12
    val regularExpression = "(?<=1)(0+)(?=1)".r
    val myList = regularExpression.findAllIn(n.toBinaryString).toList.map(_.length).sortWith(_>_)
    if (myList.length == 0){
      return 0
    }
    else {
      return myList.head
    }
  }

  def solutionOne(n:Int): Int = {

    while (n>1){
      var remainder = n % 2
    }
    return 0
  }
}
