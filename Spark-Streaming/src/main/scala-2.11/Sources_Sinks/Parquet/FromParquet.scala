package Sources_Sinks.Parquet

/**
  * Created by zaleslaw on 07.02.17.
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
    parquetFrame.select("_1").show(10)


    val innerStruct =
      StructType(StructField("_1", StringType, true) :: StructField("_2", LongType, false) :: Nil)


    val schema = StructType(
      StructField("_1", innerStruct, true) :: StructField("_2", LongType, false) :: Nil)

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

