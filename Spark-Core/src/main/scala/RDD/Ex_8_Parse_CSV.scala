package RDD

import org.apache.spark.sql.SparkSession

/**
  * Parse StateNames file with RDD
  */
object Ex_8_Parse_CSV {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ParseCSVWithRDD")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // read from file
    val stateNamesCSV = sc.textFile("/home/zaleslaw/data/StateNames.csv")
    // split / clean data
    val headerAndRows = stateNamesCSV.map(line => line.split(",").map(_.trim))
    // get header
    val header = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data = headerAndRows.filter(_ (0) != header(0))
    // splits to map (header/value pairs)
    val stateNames = data.map(splits => header.zip(splits).toMap)
    // print top-5
    stateNames.take(5).foreach(println)

    // stateNames.collect // Easy to get java.lang.OutOfMemoryError: GC overhead limit exceeded

    // you should worry about all data transformations to rdd with schema
    stateNames
      .filter(e => e("Name") == "Anna" && e("Count").toInt > 100)
      .take(5)
      .foreach(println)

    // the best way is here: try the DataFrames

  }
}
