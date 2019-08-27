package com.codility.challenges

//Challenge can be found in https://app.codility.com/programmers/lessons/2-arrays/odd_occurrences_in_array/
//Given an Array(1,2,3,2,1) returns Int 3
//Given an Array(8,8,7,1,1,7,5)returns Int 5.
//length of array always odd. returntype an Int

object OddOccurencesInArray {

  def solution(a:Array[Int]): Int = {

    if (a.length == 1) return a(0)

    val z = a.groupBy(identity).mapValues(_.size)

    val y = z.filter({case (x,y) => y%2 != 0})

    return y.head._1
  }

  // this was a solution put forward by Felipe.
  // Website link: https://app.codility.com/demo/results/training4SVDT2-YVP/

  def felipeSolution(A: Array[Int]): Int = {
    val N = A.size
    if (N<1 || N>1000000) sys.error(s"Invalid array size: $N")

    // this is a nifty trick.
    // To understand consider array of (6,3,6)
    // 6 ^ 3 -> 5
    // 5 ^ 6 -> 3  which is our unique element.
    // another example consider array of (123,189,2001,2001,123)
    // 123 ^ 189 -> 198
    // 198 ^ 2001 -> 1815
    // 1815 ^ 2001 -> 198
    // 198 ^ 123 -> 189 which is our answer

    A.foldLeft(0){(current,i) =>
      i ^ current
    }
  }

  def main(args: Array[String])={
    val y1 = Array(8,8,7,1,1,7,5)
    val y2 = Array(1,2,3,2,1)
    println(solution(y1))
    println(solution(y2))
    println(felipeSolution(y1))
    println(felipeSolution(y2))
  }

}
