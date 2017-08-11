package Demo.DStreams

import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * This class tracks all stuff about Spark and stores it to parquet files
  */
object TwitterToParquet {
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
