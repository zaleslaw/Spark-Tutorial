package Demo.Operators

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.ProcessingTime


object Filter_And_Count {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local[2]")
      .appName("SparkKafka")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "messages")
      .load()

    import org.apache.spark.sql.functions.window
    import spark.implicits._

    val result = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .as[(String, String, Timestamp)]
      .where("value % 10 == 0")
      .withWatermark("timestamp", "0 seconds")
      .groupBy(window($"timestamp", "3 seconds") as 'date)
      .count()


    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .outputMode("append")
      .format("console")
      .start()

    writer.awaitTermination()

  }


}

