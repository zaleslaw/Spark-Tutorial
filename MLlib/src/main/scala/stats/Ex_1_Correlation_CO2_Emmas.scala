
package stats

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics


/**
  * Funny correlation between CO2 concentration and amount of Emma girls in USA
  */

object Ex_1_Correlation_CO2_Emmas {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ASSERT: Files should exists
    val nationalNames = spark.read.json("/home/zaleslaw/data/nationalNames")

    import spark.implicits._

    val emmaByYear = nationalNames
      .where("Name = 'Emma' and Gender = 'F'") // because there are boys with Emma as a name
      .select($"Year", $"Count")

    val co2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/zaleslaw/data/co2.csv")


    val dataset = emmaByYear
      .join(co2, "year")
      .select($"count", $"data_mean_global".as("concentration"))


    // Pearson
    println(dataset.stat.corr("count", "concentration"))


    // for Spearman correlation we need to use ML algebra with Vectors

    // Step - 1: Make Vectors from dataframe columns using special Vector Assmebler

    val assembler = new VectorAssembler()
      .setInputCols(Array("count", "concentration"))
      .setOutputCol("features")


    val output = assembler.transform(dataset).select("features")

    output.show()

    // Step - 2: Calculate Correlation Matrix with two different methods
    val Row(coeff1: Matrix) = Correlation.corr(output, "features").head
    println("Pearson correlation matrix:\n" + coeff1.toString)

    val Row(coeff2: Matrix) = Correlation.corr(output, "features", "spearman").head
    println("Spearman correlation matrix:\n" + coeff2.toString)


    // Step - 3: Print out another stats
    // Convert initial data to mllib.Vector (instead of new ml.Vector) to use the power of old API
    val vectors = dataset.rdd
      .map (row => Vectors.dense(row.getAs[Long]("count").toDouble, row.getAs[Double]("concentration")))


    val summary = Statistics.colStats(vectors)
    println("Statistics")
    println(summary.count)
    println(summary.max.toArray.deep.mkString(" "))
    println(summary.min.toArray.deep.mkString(" "))
    println(summary.mean.toArray.deep.mkString(" "))
    println(summary.normL1.toArray.deep.mkString(" "))
    println(summary.variance.toArray.deep.mkString(" "))
  }
}

