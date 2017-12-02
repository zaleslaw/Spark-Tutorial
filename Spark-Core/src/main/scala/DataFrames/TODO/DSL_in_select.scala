package DataFrames.TODO

import org.apache.spark.sql.SparkSession

object DSL_in_select {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DSL intro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")


    import spark.implicits._ // very important import

    stateNames.select($"Gender".equalTo("'M'"),
      $"State",
      $"Year" > 1945,
      $"Count" + 10).show(100)


  }
}
