package Internals

import org.apache.spark.sql.SparkSession

object Ex_2_Persistence {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ASSERT: Files should exists
    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")


    /*    // STEP - 1: Simple query without caching
        val query = stateNames.where("Year = 2014")
        query.show
        println(query.explain(extended = true))

        // Step - 2: Cache data and see new plan
        stateNames.persist(StorageLevel.MEMORY_ONLY_SER) // or cache
        query.show()
        println(query.explain(extended = true))
        // you will see  InMemoryTableScan [Id#0, Name#1, Year#2, Gender#3, State#4, Count#5], [isnotnull(Year#2), (Year#2 = 2014)]
        // +- InMemoryRelation

        // Step - 3: Unpersist data and see the old plan
        stateNames.unpersist()
        query.show()
        println(query.explain(extended = true))
        // no InMemory stuff as you see*/

    // Step - 4: The same picture with the SparkSQL
    stateNames.createOrReplaceTempView("stateNames")
    spark.sql("CACHE TABLE stateNames")
    // Get full list of boy names
    val sqlQuery = spark.sql("SELECT DISTINCT Name FROM stateNames WHERE Gender = 'M' ORDER BY Name")
    sqlQuery.show()
    sqlQuery.explain(extended = true)

    spark.sql("UNCACHE TABLE stateNames")
    sqlQuery.show()
    sqlQuery.explain(extended = true)

  }
}


// TODO: add sample with large dataset to persist on DISK and Memory
// Light CACHE LAZY and Refresh Table