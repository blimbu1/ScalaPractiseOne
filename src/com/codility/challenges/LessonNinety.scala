package com.codility.challenges

//Not yet complete

object LessonNinety {

  def FloodDepth(a: Array[Int]):Int = {

    if (a.length == 2) return 0

    val minPoint = a.min
    val maxPoint = a.max
    val indexOfLowes = a.indexOf(minPoint)
    val indexOfHighest = a.indexOf(maxPoint)
    var arrayToSearchThrough = None: Option[Array[Int]]
    var secondHighest = None: Option[Int]
    if (indexOfHighest > indexOfLowes){
      arrayToSearchThrough = Some(a.slice(0,indexOfLowes))
      secondHighest = Some(arrayToSearchThrough.get.max)
    }
    else {
      arrayToSearchThrough = Some(a.slice(indexOfLowes+1,a.length))
      secondHighest = Some(arrayToSearchThrough.get.max)
    }

    return (secondHighest.get - minPoint)
  }

  def main(args:Array[String]) = {
    val result = FloodDepth(Array(9,11,2,7,1,8))
    val result1 = FloodDepth(Array(3,2,1,2,1,5,3,3,4,2))
    val result2 = FloodDepth(Array(5,8,666,1,5,55 ))

    println(result2)
  }

}
