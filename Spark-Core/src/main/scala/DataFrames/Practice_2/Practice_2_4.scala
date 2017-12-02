package DataFrames.Practice_2

import DataFrames.Ex_6_UDF.lambdaIsWorldWarTwoYear
import org.apache.spark.sql.SparkSession

/**
  * Parse central exam results in Russian for Yaroslavl-2016
  */

object Practice_2_4 {

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
      .option("charset", "windows-1251")
      .option("delimiter", ";")
      .csv("datasets/ege_yaroslavl.csv")

    egeResults.show(100, false)
    egeResults.cache()
    egeResults.printSchema()

    //Step-4: SparkSQL and unsuccessful classmates
    egeResults.createOrReplaceTempView("egeResults")
    val result = spark.sql("SELECT CAST(`History,spravlyaemost` AS DOUBLE), `History ,amount`, ((100- CAST(`History,spravlyaemost` AS DOUBLE))/100 * `History ,amount`) as amountOfLosers, changeNameArea(`Name area`) " +
      "as NameArea FROM egeResults WHERE `History ,amount` IS NOT NULL and  CAST(`History,spravlyaemost` AS DOUBLE) IS NOT NULL")

    result.createOrReplaceTempView("result")
    spark.sqlContext.cacheTable("result")
    result.show(300, false)
    result.printSchema()

    //Don't forget to CAST
    //Some data are dirty (with , instead . to interpret as a double)

    val result2 = spark.sql("SELECT SUM(amountOfLosers) as amountOfLosers FROM result GROUP BY NameArea")
    result2.show()
    result2.explain(extended = true)
  }
}
