package DataFrames.Practice_2

import org.apache.spark.sql.SparkSession

/**
  * Parse central exam results in Russian for Yaroslavl-2016
  */

object Practice_2_2 {

  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("EGE")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val egeResults = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("charset", "windows-1251") // <=== To work with Russian open Data
      .option("delimiter", ";") // <=== To parse file correctly
      .csv("datasets/ege_yaroslavl.csv")

    egeResults.show(100, false)
    egeResults.cache()
    egeResults.printSchema()

    //Step-1: Heads of Top-5 school by average mark on math
    import spark.implicits._
    egeResults
      .where($"Mathematics profil,GPA".isNotNull)
      .orderBy($"Mathematics profil,GPA".desc, $"Mathematics profil,amount".desc)
      .select("Head")
      .show(5, false)

    //Step-2: Max, min, average Physics,GPA for school who loves or hates geography

    import org.apache.spark.sql.functions._
    egeResults
      .where($"Physics,GPA".isNotNull)
      .withColumn("love_literature", $"Literature ,amount".isNotNull and $"Literature ,amount" > 3)
        .groupBy("love_literature")
        .agg(avg($"Physics,GPA").as("avg"), min($"Physics,GPA").as("min"), max($"Physics,GPA").as("max"))
      .show()
  }
}
