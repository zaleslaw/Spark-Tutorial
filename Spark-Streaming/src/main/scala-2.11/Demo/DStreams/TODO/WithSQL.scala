package Demo.DStreams.TODO

/**
  *
  * val hiveContext = new HiveContext(sparkContext)
  * ...
  *wordCountsDStream.foreachRDD { rdd =>
  * *
  * val wordCountsDataFrame = rdd.toDF("word”, “count")
  * *
  *wordCountsDataFrame.registerTempTable("word_counts")
  * *
  * }
  * *
  * ...
  * HiveThriftServer2.startWithContext(hiveContext)
  * *
  *
  * val stream = new StreamingContext(spark, Seconds(5))
  * val sql = new SQLContext(spark)
  * *
  * val tweets = TwitterUtils.createStream(stream, auth)
  * val transformed = tweets.filter(isEnglish).window(Minutes(1))
  *transformed.foreachRDD { rdd =>
  *rdd.map(Tweet.apply(_)).registerAsTable(“tweets”)
  * }
  * *
  * And with SparkSQL
  * *
  * SELECT text FROM tweets WHERE similarity(tweet) > 0.01
  * SELECT getClosestCountry(lat, long) FROM tweets
  *
  */
object WithSQL {

}
