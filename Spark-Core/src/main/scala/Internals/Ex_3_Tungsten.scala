package Internals

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Ex_3_Tungsten {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext


    val intSeq = 1 to math.pow(10, 7).toInt
    // Make RDD
    val intRDD = sc.parallelize(intSeq)
    println(intRDD.persist(StorageLevel.MEMORY_ONLY).count)

    import spark.sqlContext.implicits._
    println(intRDD.toDF.persist(StorageLevel.MEMORY_ONLY).count)
    while (true) {

    }
  }
}


