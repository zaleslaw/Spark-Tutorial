package DataFrames

import org.apache.spark.sql.SparkSession

object Ex_6_UDF {

  val lambdaIsWorldWarTwoYear = (year: Int) => {
    if (year >= 1939 && year <= 1945) true else false
  }

  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("UDF_UDAF")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    stateNames.cache()

    import spark.implicits._ // very important import


    // Step-1: add and register UDF function
    val isWorldWarTwoYear = spark.sqlContext.udf.register("isWorldWarTwoYear", lambdaIsWorldWarTwoYear)

    // Step-2: use UDF in dataframe
    stateNames.select($"Year", isWorldWarTwoYear($"Year")).distinct().orderBy($"Year".desc).show(100)

    // Step-3: use the same UDF in SQL expression
    stateNames.createOrReplaceTempView("stateNames")

    // Step-4: Get full list of boy names who was born during WWII
    spark.sql("SELECT DISTINCT Name FROM stateNames WHERE Gender = 'M' and isWorldWarTwoYear(Year) ORDER BY Name DESC").show(100)



  }
}
