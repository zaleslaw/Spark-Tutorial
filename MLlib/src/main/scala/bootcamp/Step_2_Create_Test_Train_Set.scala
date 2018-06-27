package bootcamp

import org.apache.spark.sql.SparkSession

object Step_2_Create_Test_Train_Set {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = getSession

    val data = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .parquet("C:\\Users\\alexey_zinovyev\\Downloads\\mlboot_dataset\\parquet")

    data.cache()
    data.show(false)

    val trainAnswers = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", "\t")
      .csv("C:\\Users\\alexey_zinovyev\\Downloads\\mlboot_dataset\\mlboot_train_answers.tsv")

    val testIds = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", "\t")
      .csv("C:\\Users\\alexey_zinovyev\\Downloads\\mlboot_dataset\\mlboot_test.tsv")

    val trainSet = data.join(trainAnswers, data.col("id").equalTo(trainAnswers.col("cuid")))
    trainSet.cache()
    trainSet.show(100, truncate = false)

    trainSet
      .select("id", "type", "diff", "target")
      .write
      .parquet("C:\\Users\\alexey_zinovyev\\Downloads\\mlboot_dataset\\train")

    val testSet = data.join(testIds, data.col("id").equalTo(testIds.col("cuid")))
    testSet.cache()
    testSet.show(100, truncate = false)

    testSet
      .select("id", "type", "diff")
      .write
      .parquet("C:\\Users\\alexey_zinovyev\\Downloads\\mlboot_dataset\\test")
  }

  private def getSession = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
