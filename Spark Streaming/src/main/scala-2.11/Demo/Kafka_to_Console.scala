package Demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime


object Kafka_to_Console {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local")
      .appName("SparkKafka")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "secondaryMessages")
      .load()

    import spark.implicits._

    val result = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .format("console")
      .start()

    writer.awaitTermination()

  }
}

