package DataFrames

import org.apache.spark.sql.SparkSession

object Ex_5_Getting_Started_with_Dataset_API {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("RDD_Intro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    import spark.implicits._
    val engineers = Seq(Engineer("Petja", "Java", 3), Engineer("Vasja", "Java", 2), Engineer("Masha", "Scala", 2)).toDS()
    val result = engineers.map(x => (x.programmingLang, x.level, x.name))
      .filter(z => z._1 == "Java")
      .groupBy("_1")
      .avg("_2")
      .sort($"avg(_2)".asc)

    result.show()

  }

  case class Engineer(name: String, programmingLang: String, level: Long)

}


