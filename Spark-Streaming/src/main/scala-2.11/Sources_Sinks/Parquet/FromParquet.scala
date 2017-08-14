package Sources_Sinks.Parquet

/**
  * Should be executed together with Ex_1_TwitterToParquet
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types._


object FromParquet {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("SparkKafka")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val parquetFrame = spark.read.parquet("result/*")
    parquetFrame.select("tag").show(10)




    val schema = StructType(
      StructField("tag", StringType, true)
        :: StructField("count", LongType, false)
        :: StructField("time", LongType, false)
        :: Nil)

    val stream = spark
      .readStream
      .schema(schema)
      .parquet("result/*")


    val result = stream.selectExpr("*")


    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .format("console")
      .start()

    writer.awaitTermination()


  }
}

