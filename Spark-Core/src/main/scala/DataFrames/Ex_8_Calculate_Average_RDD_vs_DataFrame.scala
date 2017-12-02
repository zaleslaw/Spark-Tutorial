package DataFrames

import org.apache.spark.sql.SparkSession

object Ex_8_Calculate_Average_RDD_vs_DataFrame {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Average")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val salaries = Array(
      "John 1900 January",
      "Mary 2000 January",
      "John 1800 February",
      "John 1000 March",
      "Mary 1500 February",
      "Mary 2900 March"
    )

    var rdd = spark.sparkContext.parallelize(salaries).map(_.split(" "))

    rdd.map { x => (x(0), (x(1).toFloat, 1)) }.
      reduceByKey { case ((num1, count1), (num2, count2)) =>
        (num1 + num2, count1 + count2)
      }.
      map { case (key, (num, count)) => (key, num / count) }.
      collect().foreach(println)

    val result = rdd.map { x => (x(0), (x(1).toFloat, 1)) }
    result.collect().foreach(println)

    val result2 = result.reduceByKey { case ((num1, count1), (num2, count2)) => (num1 + num2, count1 + count2) }
    result2.collect().foreach(println)

    val result3 = result2.map { case (key, (num, count)) => (key, num / count) }
    result3.collect().foreach(println)

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = rdd.map(x => (x(0), x(1))).toDF("key", "value")
    df.groupBy("key")
      .agg(avg("value"))
      .show()
  }

}
