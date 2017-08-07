object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
  }
}

HelloWorld.main(Array())

class HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello, world! Message from class")
  }
}

new HelloWorld().main(Array())

// var & val
var name = "Petja"
name = "Vasja"

val cityWhereYouWasBorn = "Leningrad"
//cityWhereYouWasBorn = "St.Petersburg" // can't be assigned


// Collections

//Immutable array
val myarray: Array[String] = new Array(10)
//Error due to immutability
//myarray = new Array(5)

myarray(1) = "Spark"

print(myarray(1))

val numberList = List(1, 2, 3, 4, 5)
numberList.indexOf(2)
//numberList.add // no such method, because it's immutable structure

// let's create new one

val numberList2 = 6 :: numberList

// or

import scala.collection.mutable.ListBuffer

var mutableList = new ListBuffer[Int]()
mutableList += 10
mutableList += 11
mutableList += 12
val numberList3 = mutableList.toList


// what's about Map?

val languageMap = Map.empty +
  (1 -> "Java") +
  (2 -> "Scala") ++
  (3 to 10).map(_ -> "PHP")

languageMap.get(3)

// lambdas and streams

val lambda1: (Int) => Boolean = e => e > 2

Array(1, 2, 3, 4, 5)
  .filter(lambda1)
  .map(e => e * 2)
  .foreach(println(_))

// let's drop dots
Array(1, 2, 3, 4, 5) filter 2.< map 2.* foreach println
