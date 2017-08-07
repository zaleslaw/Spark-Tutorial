package RDD

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Ex_6_Shared_values {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Shared values")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext


    // Step-1: Let's spread out custom dictionary
    val config = sc.broadcast(("order" -> 3, "filter" -> true))
    println(config.value)


    // Step-2: Let's publish accumulator
    val accum = sc.accumulator(0)

    // ASSERT: "/home/zaleslaw/data/ints" should exists
    val ints = sc.textFile("/home/zaleslaw/data/ints")
    ints.cache()

    ints.foreach(x => accum.add(x.toInt))

    println(accum.value)

    // Step-3: Let's publish accumulator
    // Let's use accumulator in our calculations on executor side

    // sc.textFile("/home/zaleslaw/data/ints").map(_.toInt).filter(e => e > accum.value/2).foreach(println)
    // Of course, you got java.lang.UnsupportedOperationException: Can't read accumulator value in task due to accumulators shared for writing only


    // Step-4: Let's use new version of accumulators
    val accumV2 = new LongAccumulator()
    sc.register(accumV2);
    ints.foreach(x => accumV2.add(x.toLong))


    // If skip this, catch the Caused by: java.lang.UnsupportedOperationException: Accumulator must be registered before send to executor


    if (!accumV2.isZero) {
      println("Sum is " + accumV2.sum)
      println("Avg is " + accumV2.avg)
      println("Count is " + accumV2.count)
      println("Result is " + accumV2.value)
    }

    val accumV2copy = accumV2.copy();
    sc.register(accumV2copy);

    ints.take(5).foreach(x => accumV2copy.add(x.toLong))
    accumV2.merge(accumV2copy)
    println("Merged value " + accumV2.value)

  }
}
