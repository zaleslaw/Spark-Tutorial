package Demo.DStreams

import java.util.Date

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
    System.setProperty("twitter4j.oauth.consumerKey", "wlSLT5qlmNuWUH6af3iM1MM5w")
    System.setProperty("twitter4j.oauth.consumerSecret", "yAnJvlCDxH7ypBhrxlM4DcN1Op2n9H5KSrRe53BWROJOHjueuP")
    System.setProperty("twitter4j.oauth.accessToken", "1897545698-UrA7gCK5QWpIB00XP83NYc5eYjACjDr1fmJhMwG")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "aPgF18UBBsKegghqTHo5l2ghdKqpj0pu6eIDtopXPuHat")


    val stream = TwitterUtils.createStream(ssc, None)
    // .map(gson.toJson(_)) TODO: import GSON library

    // Print user names with English as native language and have less than <threshold> friends
    // We are interested in trends among bots
    val filteredTweets = stream.filter(status => status.getLang.equals("en") && status.getUser.getFollowersCount < 30)

    // Extract tags
    val tags = filteredTweets.flatMap {
      status => status.getHashtagEntities.map(_.getText)
    }


    import spark.implicits._
    // Calculate trends
    tags.countByValue().foreachRDD {
      rdd =>
        val time = new Date().getTime
        val dataFrame = rdd.sortBy(_._2).map(tag => (tag, time)).toDF()
        dataFrame.printSchema()
        dataFrame.show()
        dataFrame.write.mode(SaveMode.Append).parquet(s"result/$time")
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
