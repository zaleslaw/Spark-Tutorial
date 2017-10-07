package DataFrames.TODO

import org.apache.spark.sql.SparkSession

object Spark_SQL_with_HiveContext_TD {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")



  }
}
