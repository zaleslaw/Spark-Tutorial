package Demo.Operators

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime


object Ex_5_OrderBy {
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

    import spark.implicits._

    val dictionary = Seq(Country("1", "Russia"), Country("2", "Germany"), Country("3", "USA")).toDS()

    val join = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(key as STRING)", "CAST(value AS INT)")
      .as[(String, String)]
      .join(dictionary, "key")

    val result = join.select($"country".alias("key"), $"value")
      .orderBy($"key".desc)


    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .format("console")
      .start()

    writer.awaitTermination()

  }

  case class Country(key: String, country: String)

}

