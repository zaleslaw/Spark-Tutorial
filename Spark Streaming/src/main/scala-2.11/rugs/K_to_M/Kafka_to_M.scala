package rugs.K_to_M

/**
  * This source is not for production use due to design constraints, e.g. infinite in-memory collection of lines read and no fault recovery.
  * MemoryStream is designed primarily for unit tests, tutorials and debugging.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

object Kafka_to_M {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local")
      .appName("SparkKafka")
      .getOrCreate()

    //spark.sparkContext.setLogLevel("ERROR")

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "messages")
      .option("failOnDataLoss", "false")
      .load()

    import spark.implicits._


    val dictionary = Seq(Country("1", "Russia"), Country("2", "Germany"), Country("3", "Belgium")).toDS()

    val join = stream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") // DUE TO due to data type mismatch: cannot cast BinaryType to IntegerType
      .selectExpr("CAST(key AS STRING)", "CAST(value AS INT)")
      .as[(String, String)]
      .join(dictionary, "key")


    val result = join.select($"country", $"value")
      .groupBy($"country")
      .sum("value")
      .select($"country".alias("key"), $"sum(value)".alias("value").cast(StringType))
      .orderBy($"key".desc)

    println(join.isStreaming)
    println(join.printSchema)


    val finalization = result.writeStream
      .queryName("table") // this query name will be the table name
      .outputMode("complete")
      .format("memory")
      //.trigger(ProcessingTime(100))
      //.option("checkpointLocation", "/home/zaleslaw/checkpoints/Kafka_to_M_aggregate_order_join")

      .start()

    Thread.sleep(1000)
    finalization.stop()
    finalization.awaitTermination(100)
    finalization.processAllAvailable() //blocking method
    spark.sql("select * from table").show()

  }

  case class Country(key: String, country: String)

}

