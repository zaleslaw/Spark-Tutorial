package RDD

import org.apache.spark.sql.SparkSession

object Ex_2_Join_with_PairRDD {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Join")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // A few developers decided to commit something
    // Define pairs <Developer name, amount of commited core lines>
    val codeRows = sc.parallelize(Seq(("Ivan", 240), ("Elena", -15), ("Petr", 39), ("Elena", 290)))

    // Let's calculate sum of code lines by developer
    codeRows.reduceByKey((x, y) => x + y).collect().foreach(println)

    // Or group items to do something else
    codeRows.groupByKey().collect().foreach(println)

    // Don't forget about joins with preferred languages

    val programmerProfiles = sc.parallelize(Seq(("Ivan", "Java"), ("Elena", "Scala"), ("Petr", "Scala")))
    programmerProfiles.saveAsSequenceFile("/home/zaleslaw/data/profiles")

    // Some(classOf[org.apache.hadoop.io.compress.SnappyCodec]) ->  java.lang.RuntimeException: native snappy library not available: this version of libhadoop was built without snappy support.

    // Read and parse data
    val joinResult = sc.sequenceFile("/home/zaleslaw/data/profiles", classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
      .map { case (x, y) => (x.toString, y.toString) } // transform from Hadoop Text to String
      .join(codeRows)


    joinResult.collect.foreach(println)

    // also we can use special operator to group values from both rdd by key
    // also we sort in DESC order

    programmerProfiles.cogroup(codeRows).sortByKey(false).collect().foreach(println)

    // If required we can get amount of values by each key
    println(joinResult.countByKey)

    // or get all values by specific key
    println(joinResult.lookup("Elena"))

    // codeRows keys only
    codeRows.keys.collect.foreach(println)

    // Print values only
    codeRows.values.collect.foreach(println)

  }
}

