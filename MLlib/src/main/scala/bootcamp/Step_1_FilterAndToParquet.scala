package bootcamp

import org.apache.spark.sql.SparkSession


object Step_1_FilterAndToParquet {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.read
      .option("charset", "windows-1251")
      .option("delimiter", "\t")
      .csv("C:\\home\\bootcamp\\mlboot_data.tsv")



    data.show(false)

    import spark.implicits._

    data
      .select($"_c0".as("id"), $"_c1".as("type"), $"_c5".as("diff"))
      .write
      .parquet("C:\\home\\bootcamp\\parquet")
  }
}
