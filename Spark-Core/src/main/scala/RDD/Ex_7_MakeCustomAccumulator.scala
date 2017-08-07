package RDD

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

/**
  * This custom accumulator will execute yet one popular aggregation operation: find max number on range of added numbers
  */
object Ex_7_MakeCustomAccumulator {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("RDD_Intro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext


    val anomalInts = sc.parallelize(Seq(1, 1, 2, 2, 3, 150, 1, 2, 3, 2, 2, 1, 1, 1, -100, 2, 2, 3, 4, 1, 2, 3, 4), 3)


    class MaxNumberAccumulator extends AccumulatorV2[Int, Int] {

      private var maxNumber = 0

      def reset(): Unit = {
        maxNumber = 0
      }

      def add(i: Int): Unit = {
        if (maxNumber < i) maxNumber = i
      }

      override def isZero: Boolean = (maxNumber == 0)

      override def copy(): AccumulatorV2[Int, Int] = new MaxNumberAccumulator()

      override def merge(other: AccumulatorV2[Int, Int]): Unit = {
        if (other.value > maxNumber) maxNumber = other.value
      }

      override def value: Int = maxNumber
    }

    val max = new MaxNumberAccumulator()
    sc.register(max);

    anomalInts.foreach(x => max.add(x))

    println(max.value)
  }


}
