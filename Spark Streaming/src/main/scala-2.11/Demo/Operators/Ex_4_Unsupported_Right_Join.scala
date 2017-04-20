package Demo.Operators

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * It doesn't wotk in SPark 2.2 with Right outer join with a streaming DataFrame/Dataset on the left is not supported;;
  */
object Ex_4_Unsupported_Right_Join {
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

    val result = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .join(dictionary, Seq("key"), "right")

    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .format("console")
      .start()


    writer.awaitTermination()

  }

  case class Country(key: String, country: String)

}

