package Demo.Great_Pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType


object Kafka_to_Kafka {
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
      .option("subscribe", "messages")
      .option("failOnDataLoss", "false")
      .load()

    import spark.implicits._

    val dictionary = Seq(Country("1", "Russia"), Country("2", "Germany"), Country("3", "USA")).toDS()

    val join = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(key as STRING)", "CAST(value AS INT)")
      .as[(String, String)]
      .join(dictionary, "key")

    val result = join.select($"country", $"value")
      .groupBy($"country")
      .sum("value")
      .select($"country".alias("key"), $"sum(value)".alias("value").cast(StringType))
      .orderBy($"key".desc)


    val writer = result.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "secondaryMessages")
      .option("checkpointLocation", "/home/zaleslaw/checkpoints")
      .outputMode(OutputMode.Complete())
      .queryName("kafkaStream")
      .start()


    writer.awaitTermination()

  }

  case class Country(key: String, country: String)

}

