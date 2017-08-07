package DataFrames

import org.apache.spark.sql.SparkSession

object Ex_1_CSV_to_Parquet_JSON {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("DataFrame intro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    // Step - 1: Extract the schema
    // Read CSV and automatically extract the schema

    val stateNames = spark.read
      .option("header", "true")
      .option("inferSchema", "true") // Id as int, count as int due to one extra pass over the data
      .csv("/home/zaleslaw/data/StateNames.csv")

    stateNames.show
    stateNames.printSchema

    stateNames.write.parquet("/home/zaleslaw/data/stateNames")

    // Step - 2: In reality it can be too expensive and CPU-burst
    // If dataset is quite big, you can infer schema manually
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

    val nationalNamesSchema = StructType(Array(
      StructField("Id", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Year", IntegerType, true),
      StructField("Gender", StringType, true),
      StructField("Count", IntegerType, true)))

    val nationalNames = spark.read
      .option("header", "true")
      .schema(nationalNamesSchema)
      .csv("/home/zaleslaw/data/NationalNames.csv")

    nationalNames.show
    nationalNames.printSchema
    nationalNames.write.json("/home/zaleslaw/data/nationalNames")
    // nationalNames.write.orc("/home/zaleslaw/data/nationalNames")
    // this is available only with HiveContext in opposite you will get an exception
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: The ORC data source must be used with Hive support enabled;

    nationalNames.cache()

    // Step - 3: Simple dataframe operations

    // filter & select & orderBy
    nationalNames
      .where("Gender == 'M'")
      .select("Name", "Year", "Count")
      .orderBy("Name", "Year")
      .show(100)


    // Registered births by year in US since 1880
    nationalNames
      .groupBy("Year")
      .sum("Count").as("Sum")
      .orderBy("Year")
      .show(200)

  }
}
