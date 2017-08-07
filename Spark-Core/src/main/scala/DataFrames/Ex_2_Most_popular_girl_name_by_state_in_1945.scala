package DataFrames

import org.apache.spark.sql.SparkSession

/**
  * To scan parquet is more effective than scan of CSV files
  */
object Ex_2_Most_popular_girl_name_by_state_in_1945 {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("RDD_Intro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ASSERT: Files should exists
    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    // Step - 3: Task - Get most popular girl name in each state in 1945
    val filteredStateNames = stateNames
      .where("Year=1945 and Gender='F'")
      .select("Name", "State", "Count")

    filteredStateNames.cache

    import spark.implicits._ // very important import

    filteredStateNames.orderBy($"state".desc, $"Count".desc).show

    import org.apache.spark.sql.functions._ // to support different functions

    val stateAndCount = filteredStateNames
      .groupBy("state")
      .agg(max("Count") as "max")

    stateAndCount.show

    // Self-join, of course
    val stateAndName = filteredStateNames
      .join(stateAndCount,
        stateAndCount.col("max").equalTo(filteredStateNames.col("Count"))
          and
          stateAndCount.col("state").equalTo(filteredStateNames.col("state")))
      .select(filteredStateNames.col("state"), $"Name".alias("name")) // should choose only String names or $Columns
      .orderBy($"state".desc, $"Count".desc)

    stateAndName.printSchema
    stateAndName.show
    stateAndName.explain
    stateAndName.explain(extended = true)

  }
}
