package RDD

import org.apache.spark.sql.SparkSession

object Ex_5_Statistics {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[]")
      .appName("Statistics")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val anomalInts = sc.parallelize(Seq(1, 1, 2, 2, 3, 150, 1, 2, 3, 2, 2, 1, 1, 1, -100, 2, 2, 3, 4, 1, 2, 3, 4), 3)
    val stats = anomalInts.stats
    val stddev = stats.stdev
    val mean = stats.mean
    println("Stddev is " + stddev + " mean is " + mean)

    val normalInts = anomalInts.filter(x => (math.abs(x - mean) < 3 * stddev))
    normalInts.collect.foreach(println)

  }
}
