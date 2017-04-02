package Demo.Operators

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime


object Filter_and_Join {
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

    val dictionary = Seq(Country("1", "Russia"), Country("2", "Germany"), Country("3", "USA")).toDS()

    val result = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .where("value % 10 == 0")
      .join(dictionary, "key")
      .select($"value".alias("key"), $"country".alias("value"))


    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .format("console")
      .start()


    writer.awaitTermination()

  }

  case class Country(key: String, country: String)

}

