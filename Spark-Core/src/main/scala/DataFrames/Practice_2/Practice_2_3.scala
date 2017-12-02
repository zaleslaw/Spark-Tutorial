package DataFrames.Practice_2

import org.apache.spark.sql.SparkSession

/**
  * Parse central exam results in Russian for Yaroslavl-2016
  */

object Practice_2_3 {

  val lambdaRecognizeSex = (head: String) => {
    if(head != null){
      val names = head.split(" ")
      if (names.size > 2 && names(2).endsWith("вич"))
        "Male"
      else if(names.size > 2 && names(2).endsWith("вна"))
        "Female"
      else
        "Unrecognized sex"
    } else {
      "Unrecognized sex"
    }

  }

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

    import org.apache.spark.sql.functions._

    //Step-3: UDF
    import spark.implicits._ // very important import
    val recognizeSex = spark.sqlContext.udf.register("recognizeSex", lambdaRecognizeSex)
    val sexAndRussianGPA = egeResults.select($"Head", recognizeSex($"Head").as("Head_sex"), $"Russian language,GPA").orderBy($"Head_sex".desc)
    sexAndRussianGPA.persist()
    sexAndRussianGPA.show(300)

    val result = sexAndRussianGPA
      .groupBy("Head_sex")
      .agg(avg("Russian language,GPA").as("avg"))

    result.show
    result.write.parquet("datasets/result")
  }
}
