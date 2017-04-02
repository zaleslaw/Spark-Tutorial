package Demo.DStreams

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Kafka_to_Console {
  def main(args: Array[String]): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "newGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val conf = new SparkConf().setAppName("First").setMaster("local[2]")
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
  }
}

