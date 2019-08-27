package com.codility.challenges

//
//Challenge description can be found in https://app.codility.com/programmers/lessons/2-arrays/cyclic_rotation/
//given an array A=(3,8,9,7,6) K=3 returns Array(9,7,6,3,8)
//given an array B= (4,8,2,1,44) K=4 returns Array(8,2,1,44,4)


object CyclicRotation {

  def solution(a: Array[Int], k:Int): Array[Int] = {

    if (a.length==0) return a

    var c = None: Option[Int]

    if (k > a.length){
      c = Some(k % a.length)
    }
    else{
      c = Some(k)
    }

    val taking = a.takeRight(c.get)
    val droping = a.dropRight(c.get)
    val newArray =  taking ++ droping

    return newArray
  }

  def main(args: Array[String]): Unit ={
    val z = Array(17,3,5,1,2,500,421,28)
    for (a <- z){
      println(solution(Array(3,8,9,7,6),a))
    }
  }

}
