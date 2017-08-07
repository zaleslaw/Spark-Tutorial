package DataFrames

import org.apache.spark.sql.SparkSession

object Ex_3_Spark_SQL {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ASSERT: Files should exists
    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    stateNames.createOrReplaceTempView("stateNames")


    // Step-1: Get full list of boy names
    spark.sql("SELECT DISTINCT Name FROM stateNames WHERE Gender = 'M' ORDER BY Name").show(100)

    // Step-2: Get proportion of state NY births in total births
    val nationalNames = spark.read.json("/home/zaleslaw/data/nationalNames")

    nationalNames.createOrReplaceTempView("nationalNames")

    val result = spark.sql("SELECT nyYear as year, stateBirths/usBirths as proportion, stateBirths, usBirths FROM (SELECT year as nyYear, SUM(count) as stateBirths FROM stateNames WHERE state = 'NY' GROUP BY year ORDER BY year) as NY" +
      " JOIN (SELECT year as usYear, SUM(count) as usBirths FROM nationalNames GROUP BY year ORDER BY year) as US ON nyYear = usYear")

    result.show(150)
    result.explain(extended = true)

  }
}
