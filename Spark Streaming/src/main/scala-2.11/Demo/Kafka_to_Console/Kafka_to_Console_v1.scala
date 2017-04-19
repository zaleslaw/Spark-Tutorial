package Demo.Kafka_to_Console

import org.apache.spark.sql.SparkSession


object Kafka_to_Console_v1 {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local")
      .appName("SparkKafka")
      .getOrCreate()

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "messages")
      .load()

    import spark.implicits._

    val result = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val writer = result.writeStream
      .format("console")
      .start()

    writer.awaitTermination()

  }
}

