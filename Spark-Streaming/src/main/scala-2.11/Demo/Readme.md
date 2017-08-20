1.*DStreams*

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Ex_1_TwitterToParquet {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TwitterToParquet").setMaster("local[2]")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")


    // TODO: extract to outer config
    import collection.JavaConversions._
    val config = ConfigFactory.load("application.conf").getConfig("twitter4j").getConfig("oauth")
    config
      .root()
      .foreach({ case (k, v) => {
        System.setProperty("twitter4j." + "oauth." + k, config.getString(k))
        println(k, v)
      }
      })

    val stream = TwitterUtils.createStream(ssc, None)
    // .map(gson.toJson(_)) TODO: import GSON library

    // Step - 1: Let's assume that users with small numbers of followers are bots
    // What trends are popular among bots now?

    // Print user names with English as native language and have less than <threshold> friends
    val filteredTweets = stream.filter(status => status.getLang.equals("en") && status.getUser.getFollowersCount < 30)

    // Extract tags
    val tags = filteredTweets.flatMap {
      status => status.getHashtagEntities.map(_.getText)
    }


    import spark.implicits._
    // Transform RDD to DF and save to Parquet
    tags.countByValue().foreachRDD {
      rdd =>
        val time = new Date().getTime
        val dataFrame = rdd.sortBy(_._2).map(tag => (tag._1, tag._2, time)).toDF()
        dataFrame.printSchema()
        val renamedDf = dataFrame.select($"_1".as("tag"), $"_2".as("count"), $"_3".as("time"))
        renamedDf.printSchema()
        renamedDf.show()
        renamedDf.write.mode(SaveMode.Append).parquet(s"result/$time")
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(30000)
    ssc.stop(false)

    println("Reading from parquet and print popular trends")

    val result = spark.read.parquet("C:\\home\\Projects\\Spark-Tutorial\\Spark-Streaming\\result\\*")


    result.select($"tag", $"count")
      .groupBy($"tag")
      .sum("count")
      .orderBy($"sum(count)".desc)
      .show(100)
  }
}

2.*Kafka*
Run Kafka and Zookeeper
Present Kafka consumers and producers and multithreading producer

3.*Kafka_to_Console with DStreams*

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: Boolean)
    )

    val conf = new SparkConf().setAppName("DStream counter").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val topicName = "messages"
    val topics = Array(topicName)

    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD {
      rdd => println("Amount of elems " + rdd.count)
    }


    ssc.start()
    ssc.awaitTermination()

3.*Kafka_to_Console with Structured Streaming*

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime


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

    val result = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .format("console")
      .start()

    writer.awaitTermination()

4.*Filter*

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

    val result = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .where("value % 10 == 0")

    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .format("console")
      .start()


    writer.awaitTermination()

5.*Filter and count*

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

6.*Join*

 val dictionary = Seq(Country("1", "Russia"), Country("2", "Germany"), Country("3", "USA")).toDS()

    val result = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .join(dictionary, "key")

    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .format("console")
      .start()


    writer.awaitTermination()

  }

Don't forget about case class out of your class

  case class Country(key: String, country: String)


7.*Group by*

    val join = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(key as STRING)", "CAST(value AS INT)")
      .join(dictionary, "key")

    val result = join.select($"country", $"value")
      .groupBy($"country")
      .sum("value")

    val writer = result.writeStream
      .trigger(ProcessingTime(3000))
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()


8.*Great pipeline*

We need one changed writer Kafka_to_Console

    val spark = SparkSession.builder
      .master("local[2]")
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

And yet one Kafka-to-Kafka


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


Run it