package Internals

import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

object Ex_1_RDD_Internals {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("RDD_Intro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    val r = 1 to 10 toArray
    // Creates RDD with 4 partitions
    val ints = sc.parallelize(r.reverse, 4)
    // Let's print internal information
    println(ints.toDebugString)

    // glom prints all partitions
    println(ints.glom().collect().deep.mkString(" ")) // to print out array in Scala

    ints.saveAsTextFile("/home/zaleslaw/data/ints") // works for windows well
    val cachedInts = sc.textFile("/home/zaleslaw/data/ints")
      .map(x => x.toInt).cache()
    println("Amount of dependencies " + cachedInts.dependencies.size)

    // Step 1: Transform each number to its square
    val squares = cachedInts
      .map(x => x * x)

    println("--Squares--")
    //squares.collect().foreach(println)

    // Step 2: Filter even numbers

    val even = squares.filter(x => x % 2 == 0)

    println("--Even numbers--")
    println(even.coalesce(2).glom().collect().deep.mkString(" "))
    println(even.partitions.size)
    println(even.coalesce(5).glom().collect().deep.mkString(" ")) // only 4 partitions due to docs
    println(even.partitions.size)
    println(even.toDebugString)
    println("Amount of dependencies " + even.dependencies.size)

    // Step - 3: Union with another RDD
    println("--Even and ints numbers")
    val union = even.union(ints)
    println(union.repartition(7).glom().collect.deep.mkString(" "))
    println(union.partitions.size)
    println("Amount of dependencies " + union.dependencies.size)
    // yeah, real empty partitions

    // Step - 4: Custom partitioner
    println("Custom partitioner")
    println(union
      .map(e => (e, e))
      .partitionBy(new EvenPartitioner(2))
      .glom()
      .collect
      .deep.mkString(" "))
  }

  class EvenPartitioner[V](partitions: Int) extends Partitioner {
    def getPartition(key: Any): Int = {
      if (key.toString.toInt % 2 == 0)
        return 0
      else return 1
    }

    override def numPartitions: Int = partitions
  }

}


