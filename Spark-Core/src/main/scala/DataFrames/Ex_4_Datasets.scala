package DataFrames

import org.apache.spark.sql.SparkSession

object Ex_4_Datasets {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Step - 1: Define schema in code with case classes


    import spark.implicits._
    // ASSERT: Files should exists
    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames").as[stateNamesRow]
    stateNames.show

    val nationalNames = spark.read.json("/home/zaleslaw/data/nationalNames").as[nationalNamesRow]
    nationalNames.show

    // Step - 2: Some typed operations

    stateNames.cache()

    val result = stateNames
      .map(x => (x.name, x.state, x.count))
      .filter(e => e._2 == "NY")
      .groupBy("_1")
      .sum("_3")
      .sort($"sum(_3)".desc)

    result.show
    result.explain(extended = true)

  }

  // Move it outside main method in other case you will get Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing sqlContext.implicits._  Support for serializing other types will be added in future releases.
  case class stateNamesRow(id: Long, name: String, year: Long, gender: String, state: String, count: Int)

  case class nationalNamesRow(id: Long, name: String, year: Long, gender: String, count: Long)

  // I made all ints in both cases to long due to encoding troubles to skip custom encoders definition
  // before that I had gotten an error: " Exception in thread "main" org.apache.spark.sql.AnalysisException: Cannot up cast `count` from bigint to int as it may truncate
  /*  The type path of the target object is:
      - field (class: "scala.Int", name: "count")
    - root class: "DataFrames.Ex_4_Datasets.nationalNamesRow"
    You can either add an explicit cast to the input data or choose a higher precision type of the field in the target object;*/
  // TODO: cool notebook as an example https://www.balabit.com/blog/spark-scala-dataset-tutorial/
  // https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-datasets.html
}
