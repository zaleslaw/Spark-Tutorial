package DataFrames

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Ex_7_UDAF {

  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("UDF_UDAF")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    stateNames.cache()

    // Step-1: Use the same UDF in SQL expression
    stateNames.createOrReplaceTempView("stateNames")

    // Step-2: Get names with max length by year
    spark.sql("SELECT Year, MAX(Name) FROM stateNames GROUP BY Year ORDER BY Year").show(100) // <= this approach doesn't work as we wish due to lexicographical order


    // Step-3: Register UDAF function
    val longestWord = spark.sqlContext.udf.register("LONGEST_WORD", new LongestWord)

    // Step-4: Get pairs <Year, Name with max length>
    spark.sql("SELECT Year, LONGEST_WORD(Name) FROM stateNames GROUP BY Year ORDER BY Year").show(100) // <= this approach doesn't work as we wish due to lexicographical order

  }

  class LongestWord extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
      new StructType().add("name", StringType, nullable = true)
    }

    override def bufferSchema: StructType = {
      new StructType().add("maxLengthWord", StringType, nullable = true)
    }

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      println(s">>> initialize (buffer: $buffer)")
      // NOTE: Scala's update used under the covers
      buffer(0) = ""
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      println(s">>> compare (buffer: $buffer -> input: $input)")
      val maxWord = buffer.getString(0)
      val currentName = input.getString(0)
      if(currentName.length > maxWord.length) {
        buffer(0) = currentName
      }

    }

    override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
      println(s">>> merge (buffer: $buffer -> row: $row)")
      val maxWord = buffer.getString(0)
      val currentName = row.getString(0)
      if(currentName.length > maxWord.length) {
        buffer(0) = currentName
      }
    }

    override def evaluate(buffer: Row): Any = {
      println(s">>> evaluate (buffer: $buffer)")
      buffer.getString(0)
    }
  }
}
