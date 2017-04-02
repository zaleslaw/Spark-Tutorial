package Demo.Great_Pipeline

import org.apache.spark.sql.SparkSession


object Kafka_to_Console {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local")
      .appName("SparkKafka")
      .getOrCreate()

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "secondaryMessages")
      .load()

    import spark.implicits._

    val result = stream.selectExpr("CAST(key AS STRING)", "CAST(value as STRING)")
      .as[(String, String)]


    val finalization = result.writeStream
      .format("console")
      .start()

    finalization.awaitTermination()

  }
}

